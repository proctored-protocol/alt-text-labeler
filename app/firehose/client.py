import logging
import os
import time

from atproto import (
    FirehoseSubscribeReposClient,
    models,
    parse_subscribe_repos_message,
)

from app.config import get_settings
from app.db import SessionLocal
from app.parsing.posts import iter_post_creates
from app.schemas import EvaluationResult
from app.services.cursor import get_saved_cursor, save_cursor
from app.services.evaluator import evaluate_post, upsert_post_evaluations
from app.services.firehose_stats import bump_firehose_stats
from app.services.overrides import get_suppressed_uris

logger = logging.getLogger(__name__)


class FirehoseWorker:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.client = None

        self.flush_interval_seconds = float(
            os.getenv("FIREHOSE_FLUSH_INTERVAL_SECONDS", "1.0")
        )
        self.max_pending_results = int(
            os.getenv("FIREHOSE_MAX_PENDING_RESULTS", "1000")
        )
        self.max_pending_commits = int(
            os.getenv("FIREHOSE_MAX_PENDING_COMMITS", "5000")
        )

        self._reset_pending()
        self._last_flush_monotonic = time.monotonic()

        logger.info(
            "firehose_worker_initialized",
            extra={
                "base_uri": self.settings.firehose_base_uri,
                "configured_cursor": self.settings.firehose_cursor,
                "dry_run": self.settings.firehose_dry_run,
                "publish_via_ozone": self.settings.publish_via_ozone,
                "publish_mode": self.settings.publish_mode,
                "mode": "evaluation_only",
                "flush_interval_seconds": self.flush_interval_seconds,
                "max_pending_results": self.max_pending_results,
                "max_pending_commits": self.max_pending_commits,
            },
        )

    def _reset_pending(self) -> None:
        self._pending_results: list[EvaluationResult] = []
        self._pending_commit_count = 0
        self._pending_post_create_count = 0
        self._pending_image_post_count = 0
        self._pending_max_seq: int | None = None
        self._pending_first_seq: int | None = None
        self._pending_eval_error_count = 0

    def _build_client(self) -> FirehoseSubscribeReposClient:
        cursor = self.settings.firehose_cursor
        with SessionLocal() as session:
            if cursor is None:
                cursor = get_saved_cursor(session)

        params = {"cursor": cursor} if cursor is not None else None

        logger.info(
            "firehose_client_building",
            extra={
                "base_uri": self.settings.firehose_base_uri,
                "cursor": cursor,
            },
        )

        return FirehoseSubscribeReposClient(
            params=params,
            base_uri=self.settings.firehose_base_uri,
        )

    def run(self) -> None:
        logger.info("firehose_worker_starting")

        reconnect_delay_seconds = 2.0

        while True:
            try:
                self.client = self._build_client()
                self.client.start(self.on_message, self.on_callback_error)

                self._flush_pending(reason="client_stopped")

                logger.warning(
                    "firehose_client_stopped_reconnecting",
                    extra={"reconnect_delay_seconds": reconnect_delay_seconds},
                )
                time.sleep(reconnect_delay_seconds)

            except Exception as exc:
                logger.exception("firehose_worker_crashed_reconnecting")

                try:
                    self._flush_pending(reason="worker_exception")
                except Exception:
                    logger.exception("firehose_pending_flush_after_exception_failed")

                error_text = str(exc)
                if "ConsumerTooSlow" in error_text:
                    logger.warning(
                        "firehose_consumer_too_slow_reconnecting",
                        extra={"reconnect_delay_seconds": reconnect_delay_seconds},
                    )

                time.sleep(reconnect_delay_seconds)
                continue

    def on_callback_error(self, exc: BaseException) -> None:
        logger.exception("firehose_callback_error", exc_info=exc)

    def on_message(self, message) -> None:
        parsed = parse_subscribe_repos_message(message)
        if not isinstance(parsed, models.ComAtprotoSyncSubscribeRepos.Commit):
            return
        self._handle_commit(parsed)

    def _handle_commit(self, commit: models.ComAtprotoSyncSubscribeRepos.Commit) -> None:
        commit_results: list[EvaluationResult] = []
        commit_post_create_count = 0
        commit_image_post_count = 0
        commit_eval_error_count = 0

        for post in iter_post_creates(commit):
            commit_post_create_count += 1

            if not post.image_alts:
                continue

            commit_image_post_count += 1

            try:
                result = evaluate_post(
                    post=post,
                    missing_label=self.settings.label_missing_alt,
                    partial_label=self.settings.label_partial_alt,
                    last_seen_seq=commit.seq,
                )
                if result is not None:
                    commit_results.append(result)

            except Exception:
                commit_eval_error_count += 1
                logger.exception(
                    "post_evaluation_failed",
                    extra={
                        "repo": commit.repo,
                        "seq": commit.seq,
                        "uri": getattr(post, "uri", None),
                    },
                )

        self._pending_commit_count += 1
        self._pending_post_create_count += commit_post_create_count
        self._pending_image_post_count += commit_image_post_count
        self._pending_eval_error_count += commit_eval_error_count
        self._pending_results.extend(commit_results)

        if self._pending_first_seq is None:
            self._pending_first_seq = commit.seq

        if self._pending_max_seq is None or commit.seq > self._pending_max_seq:
            self._pending_max_seq = commit.seq

        if self._should_flush():
            self._flush_pending(reason="threshold_reached")

    def _should_flush(self) -> bool:
        if self._pending_max_seq is None:
            return False

        if self._pending_commit_count >= self.max_pending_commits:
            return True

        if len(self._pending_results) >= self.max_pending_results:
            return True

        if (time.monotonic() - self._last_flush_monotonic) >= self.flush_interval_seconds:
            return True

        return False

    def _flush_pending(self, *, reason: str) -> None:
        if self._pending_max_seq is None:
            return

        pending_results = self._pending_results
        pending_commit_count = self._pending_commit_count
        pending_post_create_count = self._pending_post_create_count
        pending_image_post_count = self._pending_image_post_count
        pending_max_seq = self._pending_max_seq
        pending_first_seq = self._pending_first_seq
        pending_eval_error_count = self._pending_eval_error_count

        unique_uris = list({result.uri for result in pending_results})

        flush_started = time.monotonic()

        with SessionLocal() as session:
            suppressed_uris = get_suppressed_uris(session, unique_uris)

            stored_results = [
                result
                for result in pending_results
                if result.uri not in suppressed_uris
            ]

            if stored_results:
                upsert_post_evaluations(session, stored_results)

            missing_label_count = sum(
                1
                for result in stored_results
                if result.derived_label == self.settings.label_missing_alt
            )
            partial_label_count = sum(
                1
                for result in stored_results
                if result.derived_label == self.settings.label_partial_alt
            )

            bump_firehose_stats(
                session,
                commit_count=pending_commit_count,
                post_create_count=pending_post_create_count,
                image_post_count=pending_image_post_count,
                image_eval_count=len(stored_results),
                missing_label_count=missing_label_count,
                partial_label_count=partial_label_count,
            )

            save_cursor(session, pending_max_seq)
            session.commit()

        flush_seconds = round(time.monotonic() - flush_started, 4)

        logger.info(
            "firehose_flush_committed",
            extra={
                "reason": reason,
                "first_seq": pending_first_seq,
                "max_seq": pending_max_seq,
                "commit_count": pending_commit_count,
                "post_create_count": pending_post_create_count,
                "image_post_count": pending_image_post_count,
                "evaluated_result_count": len(pending_results),
                "stored_result_count": len(stored_results),
                "suppressed_count": len(suppressed_uris),
                "missing_label_count": missing_label_count,
                "partial_label_count": partial_label_count,
                "evaluation_error_count": pending_eval_error_count,
                "flush_seconds": flush_seconds,
            },
        )

        self._reset_pending()
        self._last_flush_monotonic = time.monotonic()
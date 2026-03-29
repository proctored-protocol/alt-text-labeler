import logging
import time

from atproto import (
    FirehoseSubscribeReposClient,
    models,
    parse_subscribe_repos_message,
)

from app.config import get_settings
from app.db import SessionLocal
from app.parsing.posts import iter_post_creates
from app.services.cursor import get_saved_cursor, save_cursor
from app.services.evaluator import evaluate_post, upsert_post_evaluations
from app.services.firehose_stats import bump_firehose_stats
from app.services.overrides import get_suppressed_uris

logger = logging.getLogger(__name__)


class FirehoseWorker:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.client = None

        logger.info(
            "firehose_worker_initialized",
            extra={
                "base_uri": self.settings.firehose_base_uri,
                "configured_cursor": self.settings.firehose_cursor,
                "dry_run": self.settings.firehose_dry_run,
                "publish_via_ozone": self.settings.publish_via_ozone,
                "publish_mode": self.settings.publish_mode,
                "mode": "evaluation_only",
            },
        )

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

                logger.warning(
                    "firehose_client_stopped_reconnecting",
                    extra={"reconnect_delay_seconds": reconnect_delay_seconds},
                )
                time.sleep(reconnect_delay_seconds)

            except Exception as exc:
                error_text = str(exc)

                if "ConsumerTooSlow" in error_text:
                    logger.warning(
                        "firehose_consumer_too_slow_reconnecting",
                        extra={"reconnect_delay_seconds": reconnect_delay_seconds},
                    )
                    time.sleep(reconnect_delay_seconds)
                    continue

                logger.exception("firehose_worker_crashed_reconnecting")
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
        post_create_count = 0
        image_post_count = 0
        evaluation_errors = 0
        evaluated_results = []

        for post in iter_post_creates(commit):
            post_create_count += 1

            if not post.image_alts:
                continue

            image_post_count += 1

            try:
                result = evaluate_post(
                    post=post,
                    missing_label=self.settings.label_missing_alt,
                    partial_label=self.settings.label_partial_alt,
                    last_seen_seq=commit.seq,
                )
                if result is not None:
                    evaluated_results.append(result)

            except Exception:
                evaluation_errors += 1
                logger.exception(
                    "post_evaluation_failed",
                    extra={
                        "repo": commit.repo,
                        "seq": commit.seq,
                        "uri": getattr(post, "uri", None),
                    },
                )

        try:
            with SessionLocal() as session:
                suppressed_uris = get_suppressed_uris(
                    session,
                    [result.uri for result in evaluated_results],
                )

                stored_results = [
                    result
                    for result in evaluated_results
                    if result.uri not in suppressed_uris
                ]

                if stored_results:
                    upsert_post_evaluations(session, stored_results)

                image_eval_count = len(stored_results)
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
                    commit_count=1,
                    post_create_count=post_create_count,
                    image_post_count=image_post_count,
                    image_eval_count=image_eval_count,
                    missing_label_count=missing_label_count,
                    partial_label_count=partial_label_count,
                )

                save_cursor(session, commit.seq)
                session.commit()

        except Exception:
            logger.exception(
                "commit_persist_failed",
                extra={
                    "repo": commit.repo,
                    "seq": commit.seq,
                    "post_create_count": post_create_count,
                    "image_post_count": image_post_count,
                    "evaluated_count": len(evaluated_results),
                },
            )
            return

        logger.debug(
            "commit_processed",
            extra={
                "repo": commit.repo,
                "seq": commit.seq,
                "post_create_count": post_create_count,
                "image_post_count": image_post_count,
                "evaluated_count": len(evaluated_results),
                "stored_count": len(stored_results),
                "suppressed_count": len(suppressed_uris),
                "evaluation_errors": evaluation_errors,
            },
        )
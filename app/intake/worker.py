from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from threading import Lock

from atproto import FirehoseSubscribeReposClient, models, parse_subscribe_repos_message

from app.config import get_settings
from app.db import SessionLocal, session_scope
from app.intake.repository import (
    INTAKE_CONSUMER_NAME,
    INTAKE_CONSUMER_TYPE,
    BufferedIntakePost,
    get_consumer_state,
    upsert_consumer_state,
    upsert_intake_items,
)
from app.parsing.posts import iter_post_creates


logger = logging.getLogger(__name__)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class FirehoseIntakeWorker:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.client: FirehoseSubscribeReposClient | None = None
        self.started_at = utc_now()

        self.db_batch_size = int(os.getenv("INTAKE_DB_BATCH_SIZE", "200"))
        self.flush_interval_seconds = float(os.getenv("INTAKE_FLUSH_INTERVAL_SECONDS", "0.5"))
        self.log_interval_seconds = float(os.getenv("INTAKE_LOG_INTERVAL_SECONDS", "5.0"))

        self._lock = Lock()
        self._pending_by_uri: dict[str, BufferedIntakePost] = {}
        self._pending_cursor_seq: int | None = None
        self._pending_cursor_observed_at: datetime | None = None
        self._pending_commit_count = 0
        self._pending_post_create_count = 0
        self._pending_image_post_count = 0

        self._last_flush_monotonic = time.monotonic()
        self._last_log_monotonic = time.monotonic()

        logger.info(
            "intake_worker_initialized",
            extra={
                "base_uri": self.settings.firehose_base_uri,
                "stream_name": self.settings.firehose_stream_name,
                "db_batch_size": self.db_batch_size,
                "flush_interval_seconds": self.flush_interval_seconds,
            },
        )

    def _choose_start_cursor(self) -> int | None:
        if self.settings.intake_start_cursor is not None:
            logger.info(
                "intake_worker_using_explicit_start_cursor",
                extra={"cursor": self.settings.intake_start_cursor},
            )
            return int(self.settings.intake_start_cursor)

        if not self.settings.intake_resume_from_consumer_state:
            logger.info("intake_worker_starting_without_saved_cursor")
            return None

        with SessionLocal() as session:
            state = get_consumer_state(session, INTAKE_CONSUMER_NAME)

        if state is None or state.cursor_seq is None:
            logger.info("intake_worker_no_saved_cursor_found")
            return None

        logger.info(
            "intake_worker_resuming_from_saved_cursor",
            extra={"cursor": state.cursor_seq},
        )
        return int(state.cursor_seq)

    def _build_client(self) -> FirehoseSubscribeReposClient:
        cursor = self._choose_start_cursor()
        params = {"cursor": cursor} if cursor is not None else None

        logger.info(
            "intake_worker_client_building",
            extra={
                "base_uri": self.settings.firehose_base_uri,
                "cursor": cursor,
            },
        )

        return FirehoseSubscribeReposClient(
            params=params,
            base_uri=self.settings.firehose_base_uri,
        )

    def _flush_due_locked(self, *, force: bool) -> bool:
        if self._pending_cursor_seq is None:
            return False
        if force:
            return True
        if len(self._pending_by_uri) >= self.db_batch_size:
            return True
        return (time.monotonic() - self._last_flush_monotonic) >= self.flush_interval_seconds

    def _drain_locked(self) -> tuple[
        list[BufferedIntakePost],
        int,
        datetime,
        int,
        int,
        int,
    ] | None:
        if self._pending_cursor_seq is None or self._pending_cursor_observed_at is None:
            return None

        rows = list(self._pending_by_uri.values())
        cursor_seq = self._pending_cursor_seq
        cursor_observed_at = self._pending_cursor_observed_at
        commit_count = self._pending_commit_count
        post_create_count = self._pending_post_create_count
        image_post_count = self._pending_image_post_count

        self._pending_by_uri = {}
        self._pending_cursor_seq = None
        self._pending_cursor_observed_at = None
        self._pending_commit_count = 0
        self._pending_post_create_count = 0
        self._pending_image_post_count = 0
        self._last_flush_monotonic = time.monotonic()

        return (
            rows,
            cursor_seq,
            cursor_observed_at,
            commit_count,
            post_create_count,
            image_post_count,
        )

    def _flush(self, *, force: bool, reason: str) -> None:
        with self._lock:
            if not self._flush_due_locked(force=force):
                return
            drained = self._drain_locked()

        if drained is None:
            return

        (
            rows,
            cursor_seq,
            cursor_observed_at,
            commit_count,
            post_create_count,
            image_post_count,
        ) = drained

        started = time.perf_counter()

        with session_scope() as session:
            inserted_count = upsert_intake_items(session, rows=rows)
            upsert_consumer_state(
                session,
                consumer_name=INTAKE_CONSUMER_NAME,
                consumer_type=INTAKE_CONSUMER_TYPE,
                stream_name=self.settings.firehose_stream_name,
                status="running",
                cursor_seq=cursor_seq,
                cursor_observed_at=cursor_observed_at,
                started_at=self.started_at,
                last_error_code=None,
                last_error_text=None,
            )

        flush_duration_ms = round((time.perf_counter() - started) * 1000.0, 2)

        now_mono = time.monotonic()
        if reason in {"client_stopped", "exception"} or (
            now_mono - self._last_log_monotonic >= self.log_interval_seconds
        ):
            logger.info(
                "intake_batch_flushed",
                extra={
                    "reason": reason,
                    "cursor_seq": cursor_seq,
                    "commit_count": commit_count,
                    "post_create_count": post_create_count,
                    "image_post_count": image_post_count,
                    "buffered_unique_rows": len(rows),
                    "inserted_count": inserted_count,
                    "flush_duration_ms": flush_duration_ms,
                },
            )
            self._last_log_monotonic = now_mono

    def run(self) -> None:
        reconnect_delay_seconds = 2.0

        with session_scope() as session:
            upsert_consumer_state(
                session,
                consumer_name=INTAKE_CONSUMER_NAME,
                consumer_type=INTAKE_CONSUMER_TYPE,
                stream_name=self.settings.firehose_stream_name,
                status="starting",
                cursor_seq=None,
                cursor_observed_at=None,
                started_at=self.started_at,
                last_error_code=None,
                last_error_text=None,
            )

        while True:
            try:
                self.client = self._build_client()
                self.client.start(self.on_message, self.on_callback_error)

                self._flush(force=True, reason="client_stopped")

                with session_scope() as session:
                    upsert_consumer_state(
                        session,
                        consumer_name=INTAKE_CONSUMER_NAME,
                        consumer_type=INTAKE_CONSUMER_TYPE,
                        stream_name=self.settings.firehose_stream_name,
                        status="starting",
                        cursor_seq=None,
                        cursor_observed_at=None,
                        started_at=self.started_at,
                        last_error_code="client_stopped",
                        last_error_text="Firehose client stopped unexpectedly and will reconnect.",
                    )

                logger.warning(
                    "intake_worker_client_stopped_reconnecting",
                    extra={"reconnect_delay_seconds": reconnect_delay_seconds},
                )
                time.sleep(reconnect_delay_seconds)

            except Exception as exc:
                self._flush(force=True, reason="exception")

                error_text = str(exc).strip() or exc.__class__.__name__

                with session_scope() as session:
                    upsert_consumer_state(
                        session,
                        consumer_name=INTAKE_CONSUMER_NAME,
                        consumer_type=INTAKE_CONSUMER_TYPE,
                        stream_name=self.settings.firehose_stream_name,
                        status="error",
                        cursor_seq=None,
                        cursor_observed_at=None,
                        started_at=self.started_at,
                        last_error_code=exc.__class__.__name__,
                        last_error_text=error_text,
                    )

                logger.exception("intake_worker_crashed_reconnecting")
                time.sleep(reconnect_delay_seconds)

    def on_callback_error(self, exc: BaseException) -> None:
        logger.exception("intake_worker_callback_error", exc_info=exc)

    def on_message(self, message) -> None:
        parsed = parse_subscribe_repos_message(message)
        if not isinstance(parsed, models.ComAtprotoSyncSubscribeRepos.Commit):
            return

        observed_at = utc_now()
        self._handle_commit(parsed, observed_at=observed_at)

    def _handle_commit(
        self,
        commit: models.ComAtprotoSyncSubscribeRepos.Commit,
        *,
        observed_at: datetime,
    ) -> None:
        post_create_count = 0
        image_post_count = 0
        buffered_rows: list[BufferedIntakePost] = []

        for post in iter_post_creates(commit):
            post_create_count += 1

            if post.image_count <= 0:
                continue

            image_post_count += 1
            buffered_rows.append(
                BufferedIntakePost(
                    uri=post.uri,
                    cid=post.cid,
                    author_did=post.author_did,
                    repo_did=post.repo_did,
                    record_created_at=post.record_created_at,
                    firehose_seq=commit.seq,
                    firehose_observed_at=observed_at,
                    raw_embed_type=post.raw_embed_type,
                    image_count=post.image_count,
                    image_alts_json=post.image_alts,
                )
            )

        should_flush = False

        with self._lock:
            self._pending_commit_count += 1
            self._pending_post_create_count += post_create_count
            self._pending_image_post_count += image_post_count
            self._pending_cursor_seq = commit.seq
            self._pending_cursor_observed_at = observed_at

            for row in buffered_rows:
                prev = self._pending_by_uri.get(row.uri)
                if prev is None or row.firehose_seq >= prev.firehose_seq:
                    self._pending_by_uri[row.uri] = row

            should_flush = self._flush_due_locked(force=False)

        if should_flush:
            self._flush(force=True, reason="threshold")
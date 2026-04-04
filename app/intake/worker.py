from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

from atproto import FirehoseSubscribeReposClient, models, parse_subscribe_repos_message

from app.config import get_settings
from app.db import SessionLocal, session_scope
from app.intake.repository import (
    INTAKE_CONSUMER_NAME,
    INTAKE_CONSUMER_TYPE,
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

        logger.info(
            "intake_worker_initialized",
            extra={
                "base_uri": self.settings.firehose_base_uri,
                "stream_name": self.settings.firehose_stream_name,
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
        posts = list(iter_post_creates(commit))
        image_posts = [post for post in posts if post.image_count > 0]

        with session_scope() as session:
            upsert_intake_items(
                session,
                posts=image_posts,
                firehose_seq=commit.seq,
                firehose_observed_at=observed_at,
            )
            upsert_consumer_state(
                session,
                consumer_name=INTAKE_CONSUMER_NAME,
                consumer_type=INTAKE_CONSUMER_TYPE,
                stream_name=self.settings.firehose_stream_name,
                status="running",
                cursor_seq=commit.seq,
                cursor_observed_at=observed_at,
                started_at=self.started_at,
                last_error_code=None,
                last_error_text=None,
            )

        logger.info(
            "intake_commit_committed",
            extra={
                "repo": commit.repo,
                "seq": commit.seq,
                "post_create_count": len(posts),
                "image_post_count": len(image_posts),
            },
        )
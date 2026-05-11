from __future__ import annotations

import logging
import os
import socket
import time
from datetime import datetime, timezone

from app.config import get_settings
from app.db import session_scope
from app.visibility.client import VisibilityClient, VisibilityClientError
from app.visibility.repository import (
    count_visibility_backlog,
    get_leased_visibility_check_for_worker,
    lease_visibility_batch,
    mark_old_pending_timeouts,
    mark_visibility_not_found,
    mark_visibility_not_visible,
    mark_visibility_retry_or_error,
    mark_visibility_visible,
    seed_visibility_checks,
    upsert_worker_heartbeat,
)

logger = logging.getLogger(__name__)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class VisibilityWorker:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.started_at = utc_now()
        self.host = socket.gethostname()
        self.pid = os.getpid()
        self.worker_name = f"visibility:{self.host}:{self.pid}"

        if not self.settings.test_viewer_handle or not self.settings.test_viewer_app_password:
            raise RuntimeError(
                "VisibilityWorker requires test_viewer_handle and test_viewer_app_password."
            )
        if not self.settings.verifier_labeler_did:
            raise RuntimeError("VisibilityWorker requires verifier_labeler_did.")

        self.client = VisibilityClient(
            pds_url=self.settings.bsky_pds_url,
            appview_url=self.settings.verifier_appview_url,
            viewer_identifier=self.settings.test_viewer_handle,
            viewer_password=self.settings.test_viewer_app_password,
            labeler_did=self.settings.verifier_labeler_did,
            timeout_seconds=self.settings.visibility_request_timeout_seconds,
        )

        self.batch_size = self.settings.visibility_batch_size
        self.lease_seconds = self.settings.visibility_lease_seconds
        self.idle_sleep_seconds = self.settings.visibility_idle_sleep_seconds
        self.max_attempts = self.settings.visibility_max_attempts
        self.retry_seconds = self.settings.visibility_retry_seconds
        self.max_age_seconds = self.settings.visibility_max_age_seconds
        self.seed_lookback_seconds = self.settings.visibility_seed_lookback_seconds
        self.initial_delay_seconds = self.settings.visibility_initial_delay_seconds

        logger.info(
            "visibility_worker_initialized",
            extra={
                "worker_name": self.worker_name,
                "batch_size": self.batch_size,
                "lease_seconds": self.lease_seconds,
                "retry_seconds": self.retry_seconds,
                "max_age_seconds": self.max_age_seconds,
                "seed_lookback_seconds": self.seed_lookback_seconds,
                "initial_delay_seconds": self.initial_delay_seconds,
                "mode": "baseline_5m_forced_hydration",
            },
        )

    def _heartbeat(
        self,
        *,
        status: str,
        lease_count: int | None,
        backlog_depth: int | None,
        last_error_code: str | None = None,
        last_error_text: str | None = None,
    ) -> None:
        with session_scope() as session:
            upsert_worker_heartbeat(
                session,
                worker_name=self.worker_name,
                stage="visibility",
                status=status,
                started_at=self.started_at,
                heartbeat_at=utc_now(),
                host=self.host,
                pid=self.pid,
                lease_count=lease_count,
                backlog_depth=backlog_depth,
                last_error_code=last_error_code,
                last_error_text=last_error_text,
                meta_json={
                    "mode": "baseline_5m_forced_hydration",
                    "labeler_did": self.settings.verifier_labeler_did,
                    "appview_url": self.settings.verifier_appview_url,
                    "retry_seconds": self.retry_seconds,
                    "max_age_seconds": self.max_age_seconds,
                    "seed_lookback_seconds": self.seed_lookback_seconds,
                    "initial_delay_seconds": self.initial_delay_seconds,
                },
            )

    def run(self) -> None:
        self._heartbeat(status="starting", lease_count=0, backlog_depth=0)

        while True:
            try:
                now = utc_now()

                with session_scope() as session:
                    seed_visibility_checks(
                        session,
                        max_age_seconds=self.max_age_seconds,
                        seed_lookback_seconds=self.seed_lookback_seconds,
                        initial_delay_seconds=self.initial_delay_seconds,
                        now=now,
                    )
                    mark_old_pending_timeouts(
                        session,
                        max_age_seconds=self.max_age_seconds,
                        now=now,
                    )
                    backlog_depth = count_visibility_backlog(session, now=now)
                    leased = lease_visibility_batch(
                        session,
                        worker_name=self.worker_name,
                        batch_size=self.batch_size,
                        lease_seconds=self.lease_seconds,
                        max_attempts=self.max_attempts,
                        now=now,
                    )

                if not leased:
                    self._heartbeat(
                        status="idle",
                        lease_count=0,
                        backlog_depth=backlog_depth,
                    )
                    time.sleep(self.idle_sleep_seconds)
                    continue

                self._heartbeat(
                    status="running",
                    lease_count=len(leased),
                    backlog_depth=backlog_depth,
                )

                for ref in leased:
                    self._process_one(ref.id)

                with session_scope() as session:
                    backlog_after = count_visibility_backlog(session)

                self._heartbeat(
                    status="running",
                    lease_count=0,
                    backlog_depth=backlog_after,
                )

            except Exception as exc:
                logger.exception("visibility_worker_loop_failed")
                self._heartbeat(
                    status="error",
                    lease_count=0,
                    backlog_depth=None,
                    last_error_code=exc.__class__.__name__,
                    last_error_text=str(exc),
                )
                time.sleep(2.0)

    def _process_one(self, visibility_check_id: int) -> None:
        with session_scope() as session:
            row = get_leased_visibility_check_for_worker(
                session,
                visibility_check_id=visibility_check_id,
                worker_name=self.worker_name,
            )
            if row is None:
                return

            uri = row["uri"]
            label_value = row["label_value"]
            published_at = row["published_at"]
            attempt_count = int(row["attempt_count"])

        try:
            result = self.client.check_forced_hydration(
                uri=uri,
                label_value=label_value,
            )
            finished_at = utc_now()

            with session_scope() as session:
                row = get_leased_visibility_check_for_worker(
                    session,
                    visibility_check_id=visibility_check_id,
                    worker_name=self.worker_name,
                )
                if row is None:
                    return

                if result.found_label:
                    mark_visibility_visible(
                        session,
                        visibility_check_id=visibility_check_id,
                        now=finished_at,
                        http_status=result.status_code,
                        response_json=result.payload,
                    )
                    logger.info(
                        "visibility_check_visible",
                        extra={
                            "worker_name": self.worker_name,
                            "visibility_check_id": visibility_check_id,
                            "uri": uri,
                            "label_value": label_value,
                        },
                    )
                else:
                    mark_visibility_not_visible(
                        session,
                        visibility_check_id=visibility_check_id,
                        now=finished_at,
                        http_status=result.status_code,
                        response_json=result.payload,
                    )
                    logger.info(
                        "visibility_check_not_visible_5m",
                        extra={
                            "worker_name": self.worker_name,
                            "visibility_check_id": visibility_check_id,
                            "uri": uri,
                            "label_value": label_value,
                        },
                    )

        except VisibilityClientError as exc:
            finished_at = utc_now()

            with session_scope() as session:
                row = get_leased_visibility_check_for_worker(
                    session,
                    visibility_check_id=visibility_check_id,
                    worker_name=self.worker_name,
                )
                if row is None:
                    return

                if exc.http_status == 400 and exc.error_code == "NotFound":
                    mark_visibility_not_found(
                        session,
                        visibility_check_id=visibility_check_id,
                        now=finished_at,
                        http_status=exc.http_status,
                        error_code=exc.error_code,
                        error_text=exc.error_text,
                        response_json=exc.response_json,
                    )
                    logger.info(
                        "visibility_check_not_found",
                        extra={
                            "worker_name": self.worker_name,
                            "visibility_check_id": visibility_check_id,
                            "uri": uri,
                            "label_value": label_value,
                        },
                    )
                else:
                    mark_visibility_retry_or_error(
                        session,
                        visibility_check_id=visibility_check_id,
                        attempt_count=attempt_count,
                        published_at=published_at,
                        now=finished_at,
                        retry_seconds=self.retry_seconds,
                        max_age_seconds=self.max_age_seconds,
                        max_attempts=self.max_attempts,
                        http_status=exc.http_status,
                        error_code=exc.error_code,
                        error_text=exc.error_text,
                        response_json=exc.response_json,
                        retryable=exc.retryable,
                    )
                    logger.warning(
                        "visibility_check_failed",
                        extra={
                            "worker_name": self.worker_name,
                            "visibility_check_id": visibility_check_id,
                            "uri": uri,
                            "label_value": label_value,
                            "http_status": exc.http_status,
                            "error_code": exc.error_code,
                        },
                    )
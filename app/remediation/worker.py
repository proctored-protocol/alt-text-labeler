from __future__ import annotations

import logging
import os
import socket
import time
from datetime import datetime, timezone

from app.config import get_settings
from app.db import session_scope
from app.publish.ozone_client import OzoneClient, OzonePublishError
from app.remediation.repository import (
    count_remediation_backlog,
    get_leased_remediation_for_worker,
    lease_remediation_batch,
    mark_remediation_gave_up,
    mark_remediation_not_found,
    mark_remediation_schedule_second,
    mark_remediation_visible,
    seed_remediation_jobs,
)
from app.visibility.client import VisibilityClient, VisibilityClientError
from app.visibility.repository import upsert_worker_heartbeat

logger = logging.getLogger(__name__)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class VisibilityRemediationWorker:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.started_at = utc_now()
        self.host = socket.gethostname()
        self.pid = os.getpid()
        self.worker_name = f"remediation:{self.host}:{self.pid}"

        if not self.settings.test_viewer_handle or not self.settings.test_viewer_app_password:
            raise RuntimeError(
                "VisibilityRemediationWorker requires test_viewer_handle and test_viewer_app_password."
            )
        if not self.settings.verifier_labeler_did:
            raise RuntimeError("VisibilityRemediationWorker requires verifier_labeler_did.")
        if not self.settings.ozone_base_url or not self.settings.ozone_handle or not self.settings.ozone_app_password:
            raise RuntimeError("VisibilityRemediationWorker requires Ozone credentials and base URL.")
        if not self.settings.ozone_proxy_did:
            raise RuntimeError("VisibilityRemediationWorker requires ozone_proxy_did.")

        self.visibility_client = VisibilityClient(
            pds_url=self.settings.bsky_pds_url,
            appview_url=self.settings.verifier_appview_url,
            viewer_identifier=self.settings.test_viewer_handle,
            viewer_password=self.settings.test_viewer_app_password,
            labeler_did=self.settings.verifier_labeler_did,
            timeout_seconds=self.settings.visibility_request_timeout_seconds,
        )
        self.ozone_client = OzoneClient(
            base_url=self.settings.ozone_base_url,
            pds_url=self.settings.bsky_pds_url,
            identifier=self.settings.ozone_handle,
            password=self.settings.ozone_app_password,
            proxy_did=self.settings.ozone_proxy_did,
        )

        self.batch_size = self.settings.remediation_batch_size
        self.lease_seconds = self.settings.remediation_lease_seconds
        self.idle_sleep_seconds = self.settings.remediation_idle_sleep_seconds
        self.max_attempts = self.settings.remediation_max_attempts

        self.first_delay_seconds = self.settings.remediation_first_delay_seconds
        self.second_delay_seconds = self.settings.remediation_second_delay_seconds

        self.verify_wait_seconds = max(1, int(self.settings.remediation_check_timeout_seconds))
        self.unlabel_sleep_seconds = self.settings.remediation_unlabel_sleep_seconds

        logger.info(
            "visibility_remediation_worker_initialized",
            extra={
                "worker_name": self.worker_name,
                "batch_size": self.batch_size,
                "lease_seconds": self.lease_seconds,
                "first_delay_seconds": self.first_delay_seconds,
                "second_delay_seconds": self.second_delay_seconds,
                "verify_wait_seconds": self.verify_wait_seconds,
                "unlabel_sleep_seconds": self.unlabel_sleep_seconds,
                "mode": "emit_then_single_check",
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
                stage="remediation",
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
                    "first_delay_seconds": self.first_delay_seconds,
                    "second_delay_seconds": self.second_delay_seconds,
                    "verify_wait_seconds": self.verify_wait_seconds,
                    "mode": "emit_then_single_check",
                },
            )

    def run(self) -> None:
        self._heartbeat(status="starting", lease_count=0, backlog_depth=0)

        while True:
            try:
                now = utc_now()

                with session_scope() as session:
                    seeded = seed_remediation_jobs(
                        session,
                        first_delay_seconds=self.first_delay_seconds,
                        now=now,
                    )
                    backlog_depth = count_remediation_backlog(session, now=now)
                    leased = lease_remediation_batch(
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

                logger.info(
                    "visibility_remediation_batch_leased",
                    extra={
                        "worker_name": self.worker_name,
                        "leased_count": len(leased),
                        "seeded_count": seeded,
                        "backlog_depth": backlog_depth,
                    },
                )

                self._heartbeat(
                    status="running",
                    lease_count=len(leased),
                    backlog_depth=backlog_depth,
                )

                total = len(leased)
                for idx, ref in enumerate(leased, start=1):
                    self._process_one(ref.id)
                    remaining = total - idx
                    self._heartbeat(
                        status="running",
                        lease_count=remaining,
                        backlog_depth=backlog_depth,
                    )

                with session_scope() as session:
                    backlog_after = count_remediation_backlog(session)

                self._heartbeat(
                    status="running",
                    lease_count=0,
                    backlog_depth=backlog_after,
                )

            except Exception as exc:
                logger.exception("visibility_remediation_worker_loop_failed")
                self._heartbeat(
                    status="error",
                    lease_count=0,
                    backlog_depth=None,
                    last_error_code=exc.__class__.__name__,
                    last_error_text=str(exc),
                )
                time.sleep(2.0)

    def _schedule_or_give_up(
        self,
        *,
        remediation_id: int,
        attempt_no: int,
        published_at: datetime,
        http_status: int | None,
        response_json: dict | list | None,
        error_code: str,
        error_text: str,
        unlabel_event_id: str | None,
        relabel_event_id: str | None,
    ) -> None:
        with session_scope() as session:
            row = get_leased_remediation_for_worker(
                session,
                remediation_id=remediation_id,
                worker_name=self.worker_name,
            )
            if row is None:
                return

            if attempt_no == 1:
                mark_remediation_schedule_second(
                    session,
                    remediation_id=remediation_id,
                    published_at=published_at,
                    second_delay_seconds=self.second_delay_seconds,
                    now=utc_now(),
                    http_status=http_status,
                    response_json=response_json,
                    error_code=error_code,
                    error_text=error_text,
                    unlabel_event_id=unlabel_event_id,
                    relabel_event_id=relabel_event_id,
                )
            else:
                mark_remediation_gave_up(
                    session,
                    remediation_id=remediation_id,
                    now=utc_now(),
                    http_status=http_status,
                    response_json=response_json,
                    error_code=error_code,
                    error_text=error_text,
                    unlabel_event_id=unlabel_event_id,
                    relabel_event_id=relabel_event_id,
                )

    def _process_one(self, remediation_id: int) -> None:
        with session_scope() as session:
            row = get_leased_remediation_for_worker(
                session,
                remediation_id=remediation_id,
                worker_name=self.worker_name,
            )
            if row is None:
                return

            uri = row["uri"]
            cid = row["cid"]
            label_value = row["label_value"]
            published_at = row["published_at"]
            attempt_no = int(row["attempt_count"]) + 1

        unlabel_event_id: str | None = None
        relabel_event_id: str | None = None

        try:
            unlabel_resp = self.ozone_client.negate_label(
                uri=uri,
                cid=cid,
                label_value=label_value,
                comment=f"Visibility remediation attempt {attempt_no}: remove before re-add",
            )
            if unlabel_resp.get("id") is not None:
                unlabel_event_id = str(unlabel_resp["id"])

            time.sleep(self.unlabel_sleep_seconds)

            relabel_resp = self.ozone_client.emit_label(
                uri=uri,
                cid=cid,
                label_value=label_value,
                comment=f"Visibility remediation attempt {attempt_no}: re-add label",
            )
            if relabel_resp.get("id") is not None:
                relabel_event_id = str(relabel_resp["id"])

        except OzonePublishError as exc:
            self._schedule_or_give_up(
                remediation_id=remediation_id,
                attempt_no=attempt_no,
                published_at=published_at,
                http_status=exc.http_status,
                response_json=exc.response_json,
                error_code=exc.error_code,
                error_text=exc.error_text,
                unlabel_event_id=unlabel_event_id,
                relabel_event_id=relabel_event_id,
            )

            logger.warning(
                "visibility_remediation_emit_failed",
                extra={
                    "worker_name": self.worker_name,
                    "remediation_id": remediation_id,
                    "attempt_no": attempt_no,
                    "uri": uri,
                    "label_value": label_value,
                    "http_status": exc.http_status,
                    "error_code": exc.error_code,
                },
            )
            return

        time.sleep(self.verify_wait_seconds)

        try:
            result = self.visibility_client.check_forced_hydration(
                uri=uri,
                label_value=label_value,
            )
            if result.found_label:
                with session_scope() as session:
                    row = get_leased_remediation_for_worker(
                        session,
                        remediation_id=remediation_id,
                        worker_name=self.worker_name,
                    )
                    if row is None:
                        return

                    mark_remediation_visible(
                        session,
                        remediation_id=remediation_id,
                        attempt_no=attempt_no,
                        now=utc_now(),
                        http_status=result.status_code,
                        response_json=result.payload,
                        unlabel_event_id=unlabel_event_id,
                        relabel_event_id=relabel_event_id,
                    )

                logger.info(
                    "visibility_remediation_visible",
                    extra={
                        "worker_name": self.worker_name,
                        "remediation_id": remediation_id,
                        "attempt_no": attempt_no,
                        "uri": uri,
                        "label_value": label_value,
                    },
                )
                return

            self._schedule_or_give_up(
                remediation_id=remediation_id,
                attempt_no=attempt_no,
                published_at=published_at,
                http_status=result.status_code,
                response_json=result.payload,
                error_code="still_not_visible",
                error_text="forced hydration still missing after remediation",
                unlabel_event_id=unlabel_event_id,
                relabel_event_id=relabel_event_id,
            )

            logger.warning(
                "visibility_remediation_still_not_visible",
                extra={
                    "worker_name": self.worker_name,
                    "remediation_id": remediation_id,
                    "attempt_no": attempt_no,
                    "uri": uri,
                    "label_value": label_value,
                },
            )
            return

        except VisibilityClientError as exc:
            if exc.http_status == 400 and exc.error_code == "NotFound":
                with session_scope() as session:
                    row = get_leased_remediation_for_worker(
                        session,
                        remediation_id=remediation_id,
                        worker_name=self.worker_name,
                    )
                    if row is None:
                        return

                    mark_remediation_not_found(
                        session,
                        remediation_id=remediation_id,
                        attempt_no=attempt_no,
                        now=utc_now(),
                        http_status=exc.http_status,
                        error_code=exc.error_code,
                        error_text=exc.error_text,
                        response_json=exc.response_json,
                        unlabel_event_id=unlabel_event_id,
                        relabel_event_id=relabel_event_id,
                    )

                logger.info(
                    "visibility_remediation_not_found_after_emit",
                    extra={
                        "worker_name": self.worker_name,
                        "remediation_id": remediation_id,
                        "attempt_no": attempt_no,
                        "uri": uri,
                        "label_value": label_value,
                    },
                )
                return

            self._schedule_or_give_up(
                remediation_id=remediation_id,
                attempt_no=attempt_no,
                published_at=published_at,
                http_status=exc.http_status,
                response_json=exc.response_json,
                error_code=exc.error_code,
                error_text=exc.error_text,
                unlabel_event_id=unlabel_event_id,
                relabel_event_id=relabel_event_id,
            )

            logger.warning(
                "visibility_remediation_check_failed",
                extra={
                    "worker_name": self.worker_name,
                    "remediation_id": remediation_id,
                    "attempt_no": attempt_no,
                    "uri": uri,
                    "label_value": label_value,
                    "http_status": exc.http_status,
                    "error_code": exc.error_code,
                },
            )
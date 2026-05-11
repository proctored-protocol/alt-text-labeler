from __future__ import annotations

import logging
import os
import socket
import time
from datetime import datetime, timezone

from app.config import get_settings
from app.db import session_scope
from app.publish.ozone_client import OzoneClient, OzonePublishError
from app.publish.repository import (
    count_publish_backlog,
    get_leased_publish_job_for_worker,
    insert_publish_attempt,
    lease_publish_batch,
    mark_attempt_started,
    mark_job_published,
    mark_job_rate_limited,
    mark_job_retry_or_error,
    release_job_for_local_rate_limit,
    upsert_worker_heartbeat,
)
from app.rate_limit.repository import (
    LabelWriteRateLimitConfig,
    acquire_label_write_permit,
    set_label_write_cooldown,
)

logger = logging.getLogger(__name__)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_external_created_at(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


class PublishWorker:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.started_at = utc_now()
        self.host = socket.gethostname()
        self.pid = os.getpid()
        self.worker_name = f"publish:{self.host}:{self.pid}"

        ozone_base_url = getattr(self.settings, "ozone_base_url", None)
        ozone_handle = getattr(self.settings, "ozone_handle", None)
        ozone_app_password = getattr(self.settings, "ozone_app_password", None)
        ozone_proxy_did = getattr(self.settings, "ozone_proxy_did", None)

        if not ozone_base_url or not ozone_handle or not ozone_app_password or not ozone_proxy_did:
            raise RuntimeError(
                "PublishWorker requires ozone_base_url, ozone_handle, ozone_app_password, and ozone_proxy_did."
            )

        self.client = OzoneClient(
            base_url=str(ozone_base_url),
            pds_url=str(self.settings.bsky_pds_url),
            identifier=str(ozone_handle),
            password=str(ozone_app_password),
            proxy_did=str(ozone_proxy_did),
            timeout_seconds=30.0,
        )

        self.batch_size = int(getattr(self.settings, "publish_batch_size", 50))
        self.lease_seconds = int(getattr(self.settings, "publish_lease_seconds", 90))
        self.max_attempts = int(getattr(self.settings, "publish_max_attempts", 10))
        self.idle_sleep_seconds = float(
            getattr(self.settings, "publish_idle_sleep_seconds", 1.0)
        )
        self.backoff_base_seconds = int(
            getattr(self.settings, "publish_backoff_base_seconds", 15)
        )
        self.rate_limit_scope = str(
            getattr(self.settings, "label_write_rate_limit_scope", "default") or "default"
        )
        self.rate_limit_config = LabelWriteRateLimitConfig(
            enabled=bool(getattr(self.settings, "label_write_rate_limit_enabled", True)),
            scope=self.rate_limit_scope,
            per_second=int(getattr(self.settings, "label_write_limit_per_second", 4)),
            per_hour=int(getattr(self.settings, "label_write_limit_per_hour", 9000)),
            per_day=int(getattr(self.settings, "label_write_limit_per_day", 90000)),
        )
        self.local_rate_limit_retry_seconds = int(
            getattr(self.settings, "label_write_local_retry_seconds", 30)
        )
        self.rate_limit_cooldown_seconds = int(
            getattr(self.settings, "publish_rate_limit_cooldown_seconds", 900)
        )
        self.rate_limit_max_retry_after_seconds = int(
            getattr(self.settings, "publish_rate_limit_max_retry_after_seconds", 3600)
        )
        self.rate_limit_jitter_seconds = int(
            getattr(self.settings, "publish_rate_limit_jitter_seconds", 300)
        )

        logger.info(
            "publish_worker_initialized",
            extra={
                "worker_name": self.worker_name,
                "batch_size": self.batch_size,
                "lease_seconds": self.lease_seconds,
                "max_attempts": self.max_attempts,
                "ozone_base_url": ozone_base_url,
                "created_by_did": self.client.created_by_did,
                "rate_limit_enabled": self.rate_limit_config.enabled,
                "rate_limit_scope": self.rate_limit_config.scope,
                "label_write_limit_per_second": self.rate_limit_config.per_second,
                "label_write_limit_per_hour": self.rate_limit_config.per_hour,
                "label_write_limit_per_day": self.rate_limit_config.per_day,
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
                stage="publish",
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
                    "ozone_base_url": getattr(self.settings, "ozone_base_url", None),
                    "batch_size": self.batch_size,
                    "lease_seconds": self.lease_seconds,
                    "created_by_did": self.client.created_by_did,
                    "rate_limit_enabled": self.rate_limit_config.enabled,
                    "rate_limit_scope": self.rate_limit_config.scope,
                    "label_write_limit_per_second": self.rate_limit_config.per_second,
                    "label_write_limit_per_hour": self.rate_limit_config.per_hour,
                    "label_write_limit_per_day": self.rate_limit_config.per_day,
                },
            )

    def run(self) -> None:
        self._heartbeat(status="starting", lease_count=0, backlog_depth=0)

        while True:
            try:
                now = utc_now()

                with session_scope() as session:
                    backlog_depth = count_publish_backlog(session, now=now)
                    leased = lease_publish_batch(
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
                    backlog_after = count_publish_backlog(session)

                self._heartbeat(
                    status="running",
                    lease_count=0,
                    backlog_depth=backlog_after,
                )

            except Exception as exc:
                logger.exception("publish_worker_loop_failed")
                self._heartbeat(
                    status="error",
                    lease_count=0,
                    backlog_depth=None,
                    last_error_code=exc.__class__.__name__,
                    last_error_text=str(exc),
                )
                time.sleep(2.0)

    def _process_one(self, publish_job_id: int) -> None:
        attempt_started_at = utc_now()

        with session_scope() as session:
            row = get_leased_publish_job_for_worker(
                session,
                publish_job_id=publish_job_id,
                worker_name=self.worker_name,
            )

            if row is None:
                return

            permit = acquire_label_write_permit(
                session,
                config=self.rate_limit_config,
                amount=1,
                now=attempt_started_at,
            )
            if not permit.acquired:
                retry_after_seconds = max(
                    self.local_rate_limit_retry_seconds,
                    int(permit.retry_after_seconds or 1),
                )
                release_job_for_local_rate_limit(
                    row,
                    now=attempt_started_at,
                    retry_after_seconds=retry_after_seconds,
                    reason_text=permit.reason_code or "local label-write quota exhausted",
                )
                logger.info(
                    "publish_local_rate_limit_wait",
                    extra={
                        "worker_name": self.worker_name,
                        "publish_job_id": publish_job_id,
                        "retry_after_seconds": retry_after_seconds,
                        "reason_code": permit.reason_code,
                        "cooldown_until": permit.cooldown_until.isoformat() if permit.cooldown_until else None,
                    },
                )
                return

            attempt_no = mark_attempt_started(row)
            uri = row.uri
            cid = row.cid
            label_value = row.label_value

        try:
            response_json = self.client.emit_label(
                uri=uri,
                cid=cid,
                label_value=label_value,
                comment=f"Auto-applied by alt-labeler at {attempt_started_at.isoformat()}",
                duration_in_hours=None,
            )
            finished_at = utc_now()
            external_event_id = str(response_json.get("id")) if response_json.get("id") is not None else None
            external_created_at = parse_external_created_at(response_json.get("createdAt"))

            with session_scope() as session:
                row = get_leased_publish_job_for_worker(
                    session,
                    publish_job_id=publish_job_id,
                    worker_name=self.worker_name,
                )
                if row is None:
                    return

                insert_publish_attempt(
                    session,
                    publish_job_id=publish_job_id,
                    attempt_no=attempt_no,
                    worker_name=self.worker_name,
                    started_at=attempt_started_at,
                    finished_at=finished_at,
                    result_status="published",
                    http_status=200,
                    error_code=None,
                    error_text=None,
                    external_event_id=external_event_id,
                    external_created_at=external_created_at,
                    response_json=response_json,
                    retry_after_seconds=None,
                )
                mark_job_published(
                    row,
                    published_at=finished_at,
                    external_event_id=external_event_id,
                    external_created_at=external_created_at,
                )

            logger.info(
                "publish_job_committed",
                extra={
                    "worker_name": self.worker_name,
                    "publish_job_id": publish_job_id,
                    "uri": uri,
                    "label_value": label_value,
                    "attempt_no": attempt_no,
                },
            )

        except OzonePublishError as exc:
            finished_at = utc_now()

            with session_scope() as session:
                row = get_leased_publish_job_for_worker(
                    session,
                    publish_job_id=publish_job_id,
                    worker_name=self.worker_name,
                )
                if row is None:
                    return

                if exc.http_status == 429:
                    retry_after_seconds = exc.retry_after_seconds or self.rate_limit_cooldown_seconds
                    retry_after_seconds = min(
                        max(1, int(retry_after_seconds)),
                        max(1, self.rate_limit_max_retry_after_seconds),
                    )
                    cooldown_until = set_label_write_cooldown(
                        session,
                        scope=self.rate_limit_scope,
                        cooldown_seconds=retry_after_seconds,
                        reason_code="remote_429",
                        http_status=exc.http_status,
                        last_error_text=exc.error_text,
                        now=finished_at,
                    )
                    actual_delay = mark_job_rate_limited(
                        row,
                        now=finished_at,
                        retry_after_seconds=retry_after_seconds,
                        jitter_seconds=self.rate_limit_jitter_seconds,
                        error_text=exc.error_text,
                    )
                    result_status = "rate_limited"
                else:
                    retry_after_seconds = exc.retry_after_seconds
                    cooldown_until = None
                    actual_delay = None
                    result_status = "error" if (not exc.retryable or attempt_no >= self.max_attempts) else "retry_pending"
                    mark_job_retry_or_error(
                        row,
                        error_code=exc.error_code,
                        error_text=exc.error_text,
                        max_attempts=self.max_attempts,
                        backoff_base_seconds=self.backoff_base_seconds,
                        retryable=exc.retryable,
                        now=finished_at,
                    )

                insert_publish_attempt(
                    session,
                    publish_job_id=publish_job_id,
                    attempt_no=attempt_no,
                    worker_name=self.worker_name,
                    started_at=attempt_started_at,
                    finished_at=finished_at,
                    result_status=result_status,
                    http_status=exc.http_status,
                    error_code=exc.error_code,
                    error_text=exc.error_text,
                    external_event_id=None,
                    external_created_at=None,
                    response_json=exc.response_json,
                    retry_after_seconds=retry_after_seconds,
                )

            logger.warning(
                "publish_job_failed",
                extra={
                    "worker_name": self.worker_name,
                    "publish_job_id": publish_job_id,
                    "uri": uri,
                    "label_value": label_value,
                    "attempt_no": attempt_no,
                    "http_status": exc.http_status,
                    "error_code": exc.error_code,
                    "retryable": exc.retryable,
                    "result_status": result_status,
                    "retry_after_seconds": retry_after_seconds,
                    "actual_delay_seconds": actual_delay,
                    "cooldown_until": cooldown_until.isoformat() if cooldown_until else None,
                },
            )

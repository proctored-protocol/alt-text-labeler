from __future__ import annotations

import logging
import os
import socket
import time
from datetime import datetime, timezone

from app.apply.repository import (
    count_apply_backlog,
    ensure_publish_job,
    get_active_manual_override,
    get_leased_item_for_worker,
    lease_apply_batch,
    mark_item_applied,
    mark_item_retry_or_error,
    mark_item_skipped,
    upsert_label_decision,
    upsert_worker_heartbeat,
)
from app.apply.service import evaluate_intake_item
from app.config import get_settings
from app.db import session_scope


logger = logging.getLogger(__name__)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class ApplyWorker:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.started_at = utc_now()
        self.host = socket.gethostname()
        self.pid = os.getpid()
        self.worker_name = f"apply:{self.host}:{self.pid}"

        logger.info(
            "apply_worker_initialized",
            extra={
                "worker_name": self.worker_name,
                "batch_size": self.settings.apply_batch_size,
                "lease_seconds": self.settings.apply_lease_seconds,
                "max_attempts": self.settings.apply_max_attempts,
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
                stage="apply",
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
                    "rule_version": self.settings.rule_version,
                    "missing_label": self.settings.label_missing_alt,
                    "partial_label": self.settings.label_partial_alt,
                },
            )

    def run(self) -> None:
        self._heartbeat(status="starting", lease_count=0, backlog_depth=0)

        while True:
            try:
                now = utc_now()

                with session_scope() as session:
                    backlog_depth = count_apply_backlog(session, now=now)
                    leased = lease_apply_batch(
                        session,
                        worker_name=self.worker_name,
                        batch_size=self.settings.apply_batch_size,
                        lease_seconds=self.settings.apply_lease_seconds,
                        max_attempts=self.settings.apply_max_attempts,
                        now=now,
                    )

                if not leased:
                    self._heartbeat(
                        status="idle",
                        lease_count=0,
                        backlog_depth=backlog_depth,
                    )
                    time.sleep(self.settings.apply_idle_sleep_seconds)
                    continue

                self._heartbeat(
                    status="running",
                    lease_count=len(leased),
                    backlog_depth=backlog_depth,
                )

                for ref in leased:
                    self._process_one(ref.id)

                with session_scope() as session:
                    backlog_after = count_apply_backlog(session)

                self._heartbeat(
                    status="running",
                    lease_count=0,
                    backlog_depth=backlog_after,
                )

            except Exception as exc:
                logger.exception("apply_worker_loop_failed")
                self._heartbeat(
                    status="error",
                    lease_count=0,
                    backlog_depth=None,
                    last_error_code=exc.__class__.__name__,
                    last_error_text=str(exc),
                )
                time.sleep(2.0)

    def _process_one(self, intake_item_id: int) -> None:
        try:
            with session_scope() as session:
                row = get_leased_item_for_worker(
                    session,
                    intake_item_id=intake_item_id,
                    worker_name=self.worker_name,
                )

                if row is None:
                    return

                manual_override = get_active_manual_override(
                    session,
                    uri=row.uri,
                )

                decision = evaluate_intake_item(
                    intake_item=row,
                    rule_version=self.settings.rule_version,
                    missing_label=self.settings.label_missing_alt,
                    partial_label=self.settings.label_partial_alt,
                    manual_override=manual_override,
                )

                label_decision_id = upsert_label_decision(
                    session,
                    intake_item_id=row.id,
                    uri=row.uri,
                    cid=row.cid,
                    rule_version=self.settings.rule_version,
                    image_count=decision.image_count,
                    usable_alt_count=decision.usable_alt_count,
                    decision_outcome=decision.decision_outcome,
                    decision_reason=decision.decision_reason,
                    publish_required=decision.publish_required,
                    override_applied=decision.override_applied,
                )

                if decision.publish_required and decision.label_value is not None:
                    ensure_publish_job(
                        session,
                        label_decision_id=label_decision_id,
                        uri=row.uri,
                        cid=row.cid,
                        label_value=decision.label_value,
                    )
                    mark_item_applied(row)
                else:
                    mark_item_skipped(row)

                logger.info(
                    "apply_item_committed",
                    extra={
                        "worker_name": self.worker_name,
                        "intake_item_id": row.id,
                        "uri": row.uri,
                        "decision_outcome": decision.decision_outcome,
                        "decision_reason": decision.decision_reason,
                        "publish_required": decision.publish_required,
                        "override_applied": decision.override_applied,
                    },
                )

        except Exception as exc:
            logger.exception(
                "apply_item_failed",
                extra={
                    "worker_name": self.worker_name,
                    "intake_item_id": intake_item_id,
                },
            )

            with session_scope() as session:
                row = get_leased_item_for_worker(
                    session,
                    intake_item_id=intake_item_id,
                    worker_name=self.worker_name,
                )

                if row is None:
                    return

                mark_item_retry_or_error(
                    row,
                    error_code=exc.__class__.__name__,
                    error_text=str(exc),
                    max_attempts=self.settings.apply_max_attempts,
                    backoff_base_seconds=5,
                    now=utc_now(),
                )
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import text
from sqlalchemy.orm import Session


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class LabelWriteRateLimitConfig:
    enabled: bool
    scope: str
    per_second: int
    per_hour: int
    per_day: int


@dataclass(frozen=True, slots=True)
class LabelWritePermit:
    acquired: bool
    retry_after_seconds: int
    reason_code: str | None = None
    cooldown_until: datetime | None = None


def _bucket_start(now: datetime, bucket_seconds: int) -> datetime:
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    epoch = int(now.timestamp())
    start_epoch = epoch - (epoch % bucket_seconds)
    return datetime.fromtimestamp(start_epoch, tz=timezone.utc)


def _limit_buckets(config: LabelWriteRateLimitConfig) -> list[tuple[int, int]]:
    buckets: list[tuple[int, int]] = []
    if config.per_second > 0:
        buckets.append((1, int(config.per_second)))
    if config.per_hour > 0:
        buckets.append((3600, int(config.per_hour)))
    if config.per_day > 0:
        buckets.append((86400, int(config.per_day)))
    return buckets


def _seconds_until_reset(now: datetime, bucket_start: datetime, bucket_seconds: int) -> int:
    reset_at = bucket_start + timedelta(seconds=bucket_seconds)
    return max(1, int((reset_at - now).total_seconds()) + 1)


def get_label_write_cooldown(
    session: Session,
    *,
    scope: str,
    now: datetime | None = None,
) -> LabelWritePermit | None:
    now = now or utc_now()
    row = session.execute(
        text("""
            SELECT cooldown_until, reason_code, http_status, last_error_text
            FROM label_write_rate_limit_cooldown
            WHERE scope = :scope
              AND cooldown_until > :now
        """),
        {"scope": scope, "now": now},
    ).mappings().one_or_none()

    if row is None:
        return None

    retry_after_seconds = max(1, int((row["cooldown_until"] - now).total_seconds()) + 1)
    return LabelWritePermit(
        acquired=False,
        retry_after_seconds=retry_after_seconds,
        reason_code=str(row["reason_code"] or "shared_cooldown"),
        cooldown_until=row["cooldown_until"],
    )


def acquire_label_write_permit(
    session: Session,
    *,
    config: LabelWriteRateLimitConfig,
    amount: int = 1,
    now: datetime | None = None,
) -> LabelWritePermit:
    """Try to acquire a shared label-write permit.

    This is intentionally database-backed so multiple publish/remediation workers
    share the same per-second/hour/day budget. The function is transaction-safe:
    it locks all relevant bucket rows before checking and incrementing counters.
    """
    now = now or utc_now()
    amount = max(1, int(amount))

    if not config.enabled:
        return LabelWritePermit(acquired=True, retry_after_seconds=0)

    scope = config.scope.strip() or "default"
    buckets = _limit_buckets(config)
    if not buckets:
        return LabelWritePermit(acquired=True, retry_after_seconds=0)

    cooldown = get_label_write_cooldown(session, scope=scope, now=now)
    if cooldown is not None:
        return cooldown

    bucket_rows: list[tuple[int, datetime, int, int]] = []

    # Ensure rows exist, then lock them in deterministic order to avoid deadlocks.
    for bucket_seconds, limit_count in sorted(buckets, key=lambda item: item[0], reverse=True):
        bucket_started_at = _bucket_start(now, bucket_seconds)
        session.execute(
            text("""
                INSERT INTO label_write_rate_limit_bucket (
                    scope,
                    bucket_started_at,
                    bucket_seconds,
                    used_count,
                    limit_count,
                    created_at,
                    updated_at
                )
                VALUES (
                    :scope,
                    :bucket_started_at,
                    :bucket_seconds,
                    0,
                    :limit_count,
                    :now,
                    :now
                )
                ON CONFLICT (scope, bucket_started_at, bucket_seconds)
                DO UPDATE SET
                    limit_count = EXCLUDED.limit_count,
                    updated_at = EXCLUDED.updated_at
            """),
            {
                "scope": scope,
                "bucket_started_at": bucket_started_at,
                "bucket_seconds": bucket_seconds,
                "limit_count": limit_count,
                "now": now,
            },
        )
        bucket_rows.append((bucket_seconds, bucket_started_at, limit_count, 0))

    locked = []
    for bucket_seconds, bucket_started_at, _limit_count, _unused in bucket_rows:
        row = session.execute(
            text("""
                SELECT scope, bucket_started_at, bucket_seconds, used_count, limit_count
                FROM label_write_rate_limit_bucket
                WHERE scope = :scope
                  AND bucket_started_at = :bucket_started_at
                  AND bucket_seconds = :bucket_seconds
                FOR UPDATE
            """),
            {
                "scope": scope,
                "bucket_started_at": bucket_started_at,
                "bucket_seconds": bucket_seconds,
            },
        ).mappings().one()
        locked.append(row)

    retry_after_seconds = 0
    denied_reason: str | None = None

    for row in locked:
        bucket_seconds = int(row["bucket_seconds"])
        used_count = int(row["used_count"] or 0)
        limit_count = int(row["limit_count"] or 0)
        if used_count + amount > limit_count:
            retry_after_seconds = max(
                retry_after_seconds,
                _seconds_until_reset(now, row["bucket_started_at"], bucket_seconds),
            )
            denied_reason = f"quota_exhausted_{bucket_seconds}s"

    if denied_reason is not None:
        return LabelWritePermit(
            acquired=False,
            retry_after_seconds=max(1, retry_after_seconds),
            reason_code=denied_reason,
        )

    for row in locked:
        session.execute(
            text("""
                UPDATE label_write_rate_limit_bucket
                SET
                    used_count = used_count + :amount,
                    updated_at = :now
                WHERE scope = :scope
                  AND bucket_started_at = :bucket_started_at
                  AND bucket_seconds = :bucket_seconds
            """),
            {
                "scope": scope,
                "bucket_started_at": row["bucket_started_at"],
                "bucket_seconds": int(row["bucket_seconds"]),
                "amount": amount,
                "now": now,
            },
        )

    return LabelWritePermit(acquired=True, retry_after_seconds=0)


def set_label_write_cooldown(
    session: Session,
    *,
    scope: str,
    cooldown_seconds: int,
    reason_code: str,
    http_status: int | None,
    last_error_text: str | None,
    now: datetime | None = None,
) -> datetime:
    now = now or utc_now()
    cooldown_seconds = max(1, int(cooldown_seconds))
    cooldown_until = now + timedelta(seconds=cooldown_seconds)

    session.execute(
        text("""
            INSERT INTO label_write_rate_limit_cooldown (
                scope,
                cooldown_until,
                reason_code,
                http_status,
                last_error_text,
                created_at,
                updated_at
            )
            VALUES (
                :scope,
                :cooldown_until,
                :reason_code,
                :http_status,
                :last_error_text,
                :now,
                :now
            )
            ON CONFLICT (scope)
            DO UPDATE SET
                cooldown_until = GREATEST(
                    label_write_rate_limit_cooldown.cooldown_until,
                    EXCLUDED.cooldown_until
                ),
                reason_code = EXCLUDED.reason_code,
                http_status = EXCLUDED.http_status,
                last_error_text = EXCLUDED.last_error_text,
                updated_at = EXCLUDED.updated_at
        """),
        {
            "scope": scope,
            "cooldown_until": cooldown_until,
            "reason_code": reason_code,
            "http_status": http_status,
            "last_error_text": last_error_text,
            "now": now,
        },
    )
    return cooldown_until


def prune_old_label_write_buckets(
    session: Session,
    *,
    retain_days: int = 7,
    now: datetime | None = None,
) -> int:
    now = now or utc_now()
    result = session.execute(
        text("""
            DELETE FROM label_write_rate_limit_bucket
            WHERE bucket_started_at < (:now - (:retain_days * INTERVAL '1 day'))
        """),
        {"now": now, "retain_days": int(retain_days)},
    )
    return int(result.rowcount or 0)

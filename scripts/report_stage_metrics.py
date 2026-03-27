from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import text

from app.db import engine


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    return value


def fetch_error_buckets(conn, window_minutes: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        text(
            """
            SELECT
                CASE
                    WHEN last_error = 'verification_unsuccessful' THEN 'verification_unsuccessful'
                    WHEN last_error ILIKE '%429%'
                      OR last_error ILIKE '%Too Many Requests%' THEN 'rate_limited'
                    WHEN last_error ILIKE '%getPosts returned no posts%' THEN 'post_missing_or_deleted'
                    WHEN last_error ILIKE '%createSession%'
                      OR last_error ILIKE '%accessJwt%'
                      OR last_error ILIKE '%ExpiredToken%'
                      OR last_error ILIKE '%Authorization%' THEN 'auth_or_session'
                    WHEN last_error ILIKE '%AmbiguousParameter%'
                      OR last_error ILIKE '%psycopg%'
                      OR last_error ILIKE '%sqlalchemy%' THEN 'db_or_sql'
                    ELSE 'other'
                END AS error_bucket,
                COUNT(*) AS n
            FROM label_work_item
            WHERE last_error IS NOT NULL
              AND last_error <> ''
              AND updated_at >= NOW() - (:window_minutes * INTERVAL '1 minute')
            GROUP BY 1
            ORDER BY n DESC, error_bucket
            """
        ),
        {"window_minutes": window_minutes},
    ).mappings().all()
    return [{k: normalize(v) for k, v in dict(row).items()} for row in rows]


def main() -> None:
    with engine.connect() as conn:
        current_counts = conn.execute(
            text(
                """
                SELECT
                    COUNT(*) FILTER (WHERE state = 'queued') AS queued_count,
                    COUNT(*) FILTER (WHERE state = 'leased') AS leased_count,
                    COUNT(*) FILTER (WHERE state = 'published_pending_verification') AS pending_verification_count,
                    COUNT(*) FILTER (WHERE state = 'verifying') AS verifying_count,
                    COUNT(*) FILTER (WHERE state = 'published') AS published_count,
                    COUNT(*) FILTER (WHERE state = 'verification_failed') AS verification_failed_count,
                    COUNT(*) FILTER (WHERE state = 'dead') AS dead_count
                FROM label_work_item
                """
            )
        ).mappings().one()

        window_10m = conn.execute(
            text(
                """
                SELECT
                    COUNT(*) FILTER (
                        WHERE evaluated_at >= NOW() - INTERVAL '10 minutes'
                    ) AS post_evaluation_rows_10m,

                    COUNT(*) FILTER (
                        WHERE evaluated_at >= NOW() - INTERVAL '10 minutes'
                          AND derived_label IN ('missing-alt-text', 'partial-alt-text')
                    ) AS labeled_rows_10m,

                    COUNT(*) FILTER (
                        WHERE created_at >= NOW() - INTERVAL '10 minutes'
                    ) AS queued_last_10m,

                    COUNT(*) FILTER (
                        WHERE ozone_created_at >= NOW() - INTERVAL '10 minutes'
                    ) AS emitted_last_10m,

                    COUNT(*) FILTER (
                        WHERE label_visible_at >= NOW() - INTERVAL '10 minutes'
                    ) AS verified_last_10m,

                    COUNT(*) FILTER (
                        WHERE state = 'verification_failed'
                          AND updated_at >= NOW() - INTERVAL '10 minutes'
                    ) AS verification_failed_last_10m,

                    COUNT(*) FILTER (
                        WHERE state = 'dead'
                          AND updated_at >= NOW() - INTERVAL '10 minutes'
                    ) AS dead_last_10m
                FROM label_work_item lwi
                CROSS JOIN post_evaluation pe
                """
            )
        ).mappings().one()

        window_60m = conn.execute(
            text(
                """
                SELECT
                    COUNT(*) FILTER (
                        WHERE evaluated_at >= NOW() - INTERVAL '60 minutes'
                    ) AS post_evaluation_rows_60m,

                    COUNT(*) FILTER (
                        WHERE evaluated_at >= NOW() - INTERVAL '60 minutes'
                          AND derived_label IN ('missing-alt-text', 'partial-alt-text')
                    ) AS labeled_rows_60m,

                    COUNT(*) FILTER (
                        WHERE created_at >= NOW() - INTERVAL '60 minutes'
                    ) AS queued_last_60m,

                    COUNT(*) FILTER (
                        WHERE ozone_created_at >= NOW() - INTERVAL '60 minutes'
                    ) AS emitted_last_60m,

                    COUNT(*) FILTER (
                        WHERE label_visible_at >= NOW() - INTERVAL '60 minutes'
                    ) AS verified_last_60m,

                    COUNT(*) FILTER (
                        WHERE state = 'verification_failed'
                          AND updated_at >= NOW() - INTERVAL '60 minutes'
                    ) AS verification_failed_last_60m,

                    COUNT(*) FILTER (
                        WHERE state = 'dead'
                          AND updated_at >= NOW() - INTERVAL '60 minutes'
                    ) AS dead_last_60m
                FROM label_work_item lwi
                CROSS JOIN post_evaluation pe
                """
            )
        ).mappings().one()

        error_buckets_10m = fetch_error_buckets(conn, 10)
        error_buckets_60m = fetch_error_buckets(conn, 60)

    current_counts_out = {k: normalize(v) for k, v in dict(current_counts).items()}
    window_10m_out = {k: normalize(v) for k, v in dict(window_10m).items()}
    window_60m_out = {k: normalize(v) for k, v in dict(window_60m).items()}

    print(
        json.dumps(
            {
                "generated_at_utc": utc_now_iso(),
                "current_counts": current_counts_out,
                "window_10m": window_10m_out,
                "window_60m": window_60m_out,
                "error_buckets_10m": error_buckets_10m,
                "error_buckets_60m": error_buckets_60m,
            },
            ensure_ascii=False,
            indent=2,
            default=str,
        )
    )


if __name__ == "__main__":
    main()
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


def main() -> None:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                WITH base AS (
                    SELECT
                        id,
                        state,
                        ozone_created_at,
                        final_forced_found_label,
                        final_query_found_label,
                        last_error,
                        EXTRACT(EPOCH FROM (NOW() - ozone_created_at)) AS age_seconds
                    FROM label_work_item
                    WHERE state IN ('published_pending_verification', 'verifying')
                      AND ozone_created_at IS NOT NULL
                ),
                bucketed AS (
                    SELECT
                        CASE
                            WHEN age_seconds < 120 THEN '0-2m'
                            WHEN age_seconds < 600 THEN '2-10m'
                            WHEN age_seconds < 1800 THEN '10-30m'
                            WHEN age_seconds < 3600 THEN '30-60m'
                            WHEN age_seconds < 14400 THEN '1-4h'
                            ELSE '4h+'
                        END AS bucket,
                        CASE
                            WHEN age_seconds < 120 THEN 1
                            WHEN age_seconds < 600 THEN 2
                            WHEN age_seconds < 1800 THEN 3
                            WHEN age_seconds < 3600 THEN 4
                            WHEN age_seconds < 14400 THEN 5
                            ELSE 6
                        END AS bucket_order,
                        *
                    FROM base
                )
                SELECT
                    bucket,
                    bucket_order,
                    COUNT(*) AS total_count,
                    COUNT(*) FILTER (WHERE state = 'published_pending_verification') AS pending_count,
                    COUNT(*) FILTER (WHERE state = 'verifying') AS verifying_count,
                    COUNT(*) FILTER (WHERE final_forced_found_label IS TRUE) AS forced_visible_partial_count,
                    COUNT(*) FILTER (WHERE final_query_found_label IS TRUE) AS query_visible_partial_count,
                    COUNT(*) FILTER (WHERE last_error IS NOT NULL AND last_error <> '') AS rows_with_last_error,
                    ROUND(AVG(age_seconds)::numeric, 1) AS avg_age_seconds,
                    ROUND(MAX(age_seconds)::numeric, 1) AS max_age_seconds,
                    MIN(ozone_created_at) AS oldest_emit_at,
                    MAX(ozone_created_at) AS newest_emit_at
                FROM bucketed
                GROUP BY bucket, bucket_order
                ORDER BY bucket_order
                """
            )
        ).mappings().all()

        totals = conn.execute(
            text(
                """
                SELECT
                    COUNT(*) FILTER (WHERE state = 'published_pending_verification') AS pending_total,
                    COUNT(*) FILTER (WHERE state = 'verifying') AS verifying_total,
                    COUNT(*) FILTER (
                        WHERE state IN ('published_pending_verification', 'verifying')
                          AND ozone_created_at IS NOT NULL
                    ) AS combined_total
                FROM label_work_item
                """
            )
        ).mappings().one()

    output_rows = [{k: normalize(v) for k, v in dict(row).items()} for row in rows]
    totals_out = {k: normalize(v) for k, v in dict(totals).items()}

    print(
        json.dumps(
            {
                "generated_at_utc": utc_now_iso(),
                "totals": totals_out,
                "age_buckets": output_rows,
            },
            ensure_ascii=False,
            indent=2,
            default=str,
        )
    )


if __name__ == "__main__":
    main()
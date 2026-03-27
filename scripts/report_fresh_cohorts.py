from __future__ import annotations

import argparse
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
    parser = argparse.ArgumentParser(
        description="Report fresh emitted-label cohorts and visibility outcomes."
    )
    parser.add_argument("--lookback-hours", type=int, default=24)
    args = parser.parse_args()

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                WITH base AS (
                    SELECT
                        id,
                        state,
                        ozone_created_at,
                        label_visible_at,
                        final_forced_found_label,
                        final_query_found_label,
                        EXTRACT(EPOCH FROM (NOW() - ozone_created_at)) AS age_seconds
                    FROM label_work_item
                    WHERE ozone_created_at IS NOT NULL
                      AND ozone_created_at >= NOW() - (:lookback_hours * INTERVAL '1 hour')
                ),
                bucketed AS (
                    SELECT
                        CASE
                            WHEN age_seconds < 120 THEN '0-2m'
                            WHEN age_seconds < 600 THEN '2-10m'
                            WHEN age_seconds < 1800 THEN '10-30m'
                            WHEN age_seconds < 3600 THEN '30-60m'
                            ELSE '60m+'
                        END AS bucket,
                        CASE
                            WHEN age_seconds < 120 THEN 1
                            WHEN age_seconds < 600 THEN 2
                            WHEN age_seconds < 1800 THEN 3
                            WHEN age_seconds < 3600 THEN 4
                            ELSE 5
                        END AS bucket_order,
                        *
                    FROM base
                )
                SELECT
                    bucket,
                    bucket_order,
                    COUNT(*) AS emitted_count,
                    COUNT(*) FILTER (WHERE state = 'published') AS verified_published_count,
                    COUNT(*) FILTER (WHERE state = 'published_pending_verification') AS pending_verification_count,
                    COUNT(*) FILTER (WHERE state = 'verifying') AS verifying_count,
                    COUNT(*) FILTER (WHERE state = 'verification_failed') AS verification_failed_count,
                    COUNT(*) FILTER (WHERE final_forced_found_label IS TRUE) AS forced_visible_count,
                    COUNT(*) FILTER (WHERE final_query_found_label IS TRUE) AS query_visible_count,
                    COUNT(*) FILTER (
                        WHERE final_forced_found_label IS TRUE
                           OR final_query_found_label IS TRUE
                    ) AS any_visible_count,
                    MIN(ozone_created_at) AS oldest_emit_at,
                    MAX(ozone_created_at) AS newest_emit_at
                FROM bucketed
                GROUP BY bucket, bucket_order
                ORDER BY bucket_order
                """
            ),
            {"lookback_hours": args.lookback_hours},
        ).mappings().all()

    output_rows: list[dict[str, Any]] = []
    for row in rows:
        item = {k: normalize(v) for k, v in dict(row).items()}
        emitted = item["emitted_count"] or 0
        forced = item["forced_visible_count"] or 0
        query = item["query_visible_count"] or 0
        any_visible = item["any_visible_count"] or 0
        published = item["verified_published_count"] or 0

        item["forced_visible_pct"] = round((forced / emitted) * 100, 2) if emitted else None
        item["query_visible_pct"] = round((query / emitted) * 100, 2) if emitted else None
        item["any_visible_pct"] = round((any_visible / emitted) * 100, 2) if emitted else None
        item["published_pct"] = round((published / emitted) * 100, 2) if emitted else None
        output_rows.append(item)

    print(
        json.dumps(
            {
                "generated_at_utc": utc_now_iso(),
                "lookback_hours": args.lookback_hours,
                "cohorts": output_rows,
            },
            ensure_ascii=False,
            indent=2,
            default=str,
        )
    )


if __name__ == "__main__":
    main()
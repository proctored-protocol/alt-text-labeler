from __future__ import annotations

import json
from datetime import datetime, timezone

from sqlalchemy import text

from app.db import engine


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def print_section(title: str) -> None:
    print()
    print(f"=== {title} ===")


def main() -> None:
    with engine.connect() as conn:
        queue_counts = conn.execute(
            text(
                """
                SELECT state, COUNT(*) AS n
                FROM label_work_item
                GROUP BY state
                ORDER BY state
                """
            )
        ).mappings().all()

        publish_counts = conn.execute(
            text(
                """
                SELECT state, COUNT(*) AS n
                FROM publish_job
                GROUP BY state
                ORDER BY state
                """
            )
        ).mappings().all()

        throughput_10m = conn.execute(
            text(
                """
                SELECT
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
                FROM label_work_item
                """
            )
        ).mappings().one()

        visibility_stats_10m = conn.execute(
            text(
                """
                SELECT
                    COUNT(*) FILTER (
                        WHERE final_forced_found_label IS TRUE
                          AND label_visible_at >= NOW() - INTERVAL '10 minutes'
                    ) AS forced_visible_last_10m,

                    COUNT(*) FILTER (
                        WHERE final_query_found_label IS TRUE
                          AND label_visible_at >= NOW() - INTERVAL '10 minutes'
                    ) AS query_visible_last_10m
                FROM label_work_item
                """
            )
        ).mappings().one()

        worker_leases = conn.execute(
            text(
                """
                SELECT
                    leased_by,
                    COUNT(*) AS n,
                    MIN(leased_until) AS earliest_lease_expiry,
                    MAX(updated_at) AS latest_update
                FROM label_work_item
                WHERE state IN ('leased', 'verifying')
                GROUP BY leased_by
                ORDER BY leased_by
                """
            )
        ).mappings().all()

        recent_items = conn.execute(
            text(
                """
                SELECT
                    id,
                    post_url,
                    record_created_at,
                    label_value,
                    state,
                    ozone_created_at,
                    label_visible_at,
                    final_forced_found_label,
                    final_query_found_label,
                    manual_success,
                    last_error,
                    updated_at
                FROM label_work_item
                ORDER BY updated_at DESC
                LIMIT 25
                """
            )
        ).mappings().all()

    print(f"generated_at_utc: {utc_now().isoformat()}")

    print_section("label_work_item counts")
    for row in queue_counts:
        print(dict(row))

    print_section("publish_job counts")
    for row in publish_counts:
        print(dict(row))

    print_section("10-minute throughput")
    print(dict(throughput_10m))
    print(dict(visibility_stats_10m))

    print_section("leased jobs by worker")
    if worker_leases:
        for row in worker_leases:
            print(dict(row))
    else:
        print("(none)")

    print_section("recent label_work_item rows")
    for row in recent_items:
        print(json.dumps(dict(row), ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
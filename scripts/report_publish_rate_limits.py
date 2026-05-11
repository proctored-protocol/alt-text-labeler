from __future__ import annotations

from sqlalchemy import text

from app.db import get_engine


def print_rows(title: str, sql: str) -> None:
    print(f"\n--- {title} ---")
    with get_engine().connect() as conn:
        rows = conn.execute(text(sql)).mappings().all()
    for row in rows:
        print(dict(row))


def main() -> None:
    print_rows(
        "label_write_cooldown",
        """
        SELECT scope, cooldown_until, now() - updated_at AS updated_age,
               reason_code, http_status, left(coalesce(last_error_text,''), 200) AS last_error_text
        FROM label_write_rate_limit_cooldown
        ORDER BY scope
        """,
    )

    print_rows(
        "label_write_buckets_current",
        """
        SELECT scope, bucket_seconds, bucket_started_at,
               used_count, limit_count,
               round(100.0 * used_count / greatest(limit_count, 1), 2) AS pct_used,
               bucket_started_at + (bucket_seconds * interval '1 second') AS resets_at
        FROM label_write_rate_limit_bucket
        WHERE bucket_started_at >= now() - interval '25 hours'
        ORDER BY bucket_seconds DESC, bucket_started_at DESC, scope
        LIMIT 80
        """,
    )

    print_rows(
        "publish_attempts_last_2h",
        """
        SELECT result_status, http_status, error_code,
               count(*) AS attempts,
               count(DISTINCT publish_job_id) AS unique_jobs,
               min(started_at) AS first_seen,
               max(finished_at) AS last_seen,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (finished_at - started_at))) AS p50_seconds,
               percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (finished_at - started_at))) AS p95_seconds,
               max(retry_after_seconds) AS max_retry_after_seconds
        FROM publish_attempt
        WHERE started_at >= now() - interval '2 hours'
        GROUP BY result_status, http_status, error_code
        ORDER BY attempts DESC
        """,
    )

    print_rows(
        "publish_jobs_by_status",
        """
        SELECT status, last_error_code, count(*) AS n,
               min(created_at) AS oldest_created_at,
               min(next_attempt_at) AS oldest_next_attempt_at,
               max(next_attempt_at) AS newest_next_attempt_at
        FROM publish_job
        GROUP BY status, last_error_code
        ORDER BY status, n DESC
        """,
    )

    print_rows(
        "publish_demand_by_hour",
        """
        SELECT date_trunc('hour', created_at) AS hour,
               count(*) AS publish_jobs_created
        FROM publish_job
        WHERE created_at >= now() - interval '48 hours'
        GROUP BY 1
        ORDER BY 1 DESC
        """,
    )


if __name__ == "__main__":
    main()

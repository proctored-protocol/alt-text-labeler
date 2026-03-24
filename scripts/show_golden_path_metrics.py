#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    args = parser.parse_args()

    db_path = Path(args.db)
    conn = sqlite3.connect(db_path)

    print("=== totals ===")
    row = conn.execute(
        """
        select
          count(*) as total_posts,
          sum(case when success = 1 then 1 else 0 end) as success_ok,
          sum(case when candidate_error = 1 then 1 else 0 end) as candidate_errors,
          sum(case when processing_error is not null and candidate_error = 0 then 1 else 0 end) as infrastructure_errors,
          sum(case when final_subscriber_found_label = 1 then 1 else 0 end) as subscriber_ok
        from processed_posts
        """
    ).fetchone()
    print(
        {
            "total_posts": row[0] or 0,
            "success_ok": row[1] or 0,
            "candidate_errors": row[2] or 0,
            "infrastructure_errors": row[3] or 0,
            "subscriber_ok": row[4] or 0,
        }
    )

    print("\n=== rolling windows ===")
    for limit in (25, 50, 100):
        row = conn.execute(
            """
            with recent as (
              select success, candidate_error
              from processed_posts
              order by processed_at desc
              limit ?
            )
            select
              count(*) filter (where candidate_error = 0),
              sum(case when candidate_error = 0 and success = 1 then 1 else 0 end)
            from recent
            """,
            (limit,),
        ).fetchone()
        n = row[0] or 0
        ok = row[1] or 0
        rate = (ok / n) if n else 1.0
        print({"window": limit, "n": n, "ok": ok, "success_rate": round(rate, 4)})

    print("\n=== errors by class ===")
    rows = conn.execute(
        """
        select processing_error, count(*)
        from processed_posts
        where processing_error is not null
        group by processing_error
        order by count(*) desc
        limit 20
        """
    ).fetchall()
    for error_text, count_ in rows:
        print({"count": count_, "error_text": error_text[:300]})

    print("\n=== lag summary ===")
    row = conn.execute(
        """
        with lags as (
          select
            (julianday(first_forced_true_at) - julianday(replace(replace(ozone_created_at,'T',' '),'Z',''))) * 86400.0 as forced_s,
            (julianday(first_query_true_at) - julianday(replace(replace(ozone_created_at,'T',' '),'Z',''))) * 86400.0 as query_s
          from processed_posts
          where ozone_created_at is not null
            and first_forced_true_at is not null
            and first_query_true_at is not null
        )
        select
          count(*),
          round(avg(forced_s), 2),
          round(min(forced_s), 2),
          round(max(forced_s), 2),
          round(avg(query_s), 2),
          round(min(query_s), 2),
          round(max(query_s), 2)
        from lags
        """
    ).fetchone()
    print(
        {
            "n": row[0] or 0,
            "avg_forced_s": row[1],
            "min_forced_s": row[2],
            "max_forced_s": row[3],
            "avg_query_s": row[4],
            "min_query_s": row[5],
            "max_query_s": row[6],
        }
    )

    print("\n=== recent processed ===")
    rows = conn.execute(
        """
        select
          processed_at,
          post_url,
          label_value,
          success,
          final_forced_found_label,
          final_query_found_label,
          final_subscriber_found_label,
          processing_error
        from processed_posts
        order by processed_at desc
        limit 20
        """
    ).fetchall()
    for row in rows:
        print(
            {
                "processed_at": row[0],
                "post_url": row[1],
                "label_value": row[2],
                "success": bool(row[3]),
                "final_forced_found_label": bool(row[4]),
                "final_query_found_label": bool(row[5]),
                "final_subscriber_found_label": bool(row[6]),
                "processing_error": row[7],
            }
        )

    print("\n=== reset events ===")
    rows = conn.execute(
        """
        select created_at, payload_json
        from worker_events
        where event_type = 'labeler_reset'
        order by created_at desc
        limit 20
        """
    ).fetchall()
    for created_at, payload_json in rows:
        print({"created_at": created_at, "payload_json": payload_json[:400]})


if __name__ == "__main__":
    main()
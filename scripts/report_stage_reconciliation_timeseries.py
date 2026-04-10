from __future__ import annotations

import json
import os

from sqlalchemy import text

from app.db import get_engine


def main() -> None:
    lookback_minutes = int(os.getenv("LOOKBACK_MINUTES", "60"))
    end_lag_minutes = int(os.getenv("END_LAG_MINUTES", "20"))

    with get_engine().connect() as conn:
        rows = conn.execute(text(f"""
            WITH head AS (
                SELECT
                    date_trunc('minute', bucket_second) AS minute_bucket,
                    SUM(post_count) AS head_post_creates,
                    SUM(image_post_count) AS head_image_posts,
                    SUM(missing_alt_post_count) AS head_missing_alt_posts,
                    SUM(partial_alt_post_count) AS head_partial_alt_posts
                FROM firehose_head_sample
                WHERE bucket_second >= NOW() - INTERVAL '{lookback_minutes + end_lag_minutes} minutes'
                  AND bucket_second <  NOW() - INTERVAL '{end_lag_minutes} minutes'
                GROUP BY 1
            ),
            intake AS (
                SELECT
                    date_trunc('minute', firehose_observed_at) AS minute_bucket,
                    COUNT(*) AS intake_rows,
                    COUNT(*) FILTER (WHERE apply_status = 'applied') AS intake_applied,
                    COUNT(*) FILTER (WHERE apply_status = 'skipped') AS intake_skipped,
                    COUNT(*) FILTER (WHERE apply_status = 'pending') AS intake_pending,
                    COUNT(*) FILTER (WHERE apply_status = 'leased') AS intake_leased
                FROM intake_item
                WHERE firehose_observed_at >= NOW() - INTERVAL '{lookback_minutes + end_lag_minutes} minutes'
                  AND firehose_observed_at <  NOW() - INTERVAL '{end_lag_minutes} minutes'
                GROUP BY 1
            ),
            publish AS (
                SELECT
                    date_trunc('minute', ii.firehose_observed_at) AS minute_bucket,
                    COUNT(DISTINCT pj.id) AS publish_jobs_total,
                    COUNT(DISTINCT pj.id) FILTER (WHERE pj.status = 'published') AS publish_jobs_published,
                    COUNT(DISTINCT pj.id) FILTER (WHERE pj.status = 'pending') AS publish_jobs_pending,
                    COUNT(DISTINCT pj.id) FILTER (WHERE pj.status = 'leased') AS publish_jobs_leased
                FROM intake_item ii
                LEFT JOIN publish_job pj
                  ON pj.uri = ii.uri
                WHERE ii.firehose_observed_at >= NOW() - INTERVAL '{lookback_minutes + end_lag_minutes} minutes'
                  AND ii.firehose_observed_at <  NOW() - INTERVAL '{end_lag_minutes} minutes'
                  AND ii.apply_status = 'applied'
                GROUP BY 1
            )
            SELECT
                COALESCE(h.minute_bucket, i.minute_bucket, p.minute_bucket) AS minute_bucket,
                COALESCE(h.head_post_creates, 0) AS head_post_creates,
                COALESCE(h.head_image_posts, 0) AS head_image_posts,
                COALESCE(h.head_missing_alt_posts, 0) AS head_missing_alt_posts,
                COALESCE(h.head_partial_alt_posts, 0) AS head_partial_alt_posts,
                COALESCE(i.intake_rows, 0) AS intake_rows,
                COALESCE(i.intake_applied, 0) AS intake_applied,
                COALESCE(i.intake_skipped, 0) AS intake_skipped,
                COALESCE(i.intake_pending, 0) AS intake_pending,
                COALESCE(i.intake_leased, 0) AS intake_leased,
                COALESCE(p.publish_jobs_total, 0) AS publish_jobs_total,
                COALESCE(p.publish_jobs_published, 0) AS publish_jobs_published,
                COALESCE(p.publish_jobs_pending, 0) AS publish_jobs_pending,
                COALESCE(p.publish_jobs_leased, 0) AS publish_jobs_leased
            FROM head h
            FULL OUTER JOIN intake i
              ON i.minute_bucket = h.minute_bucket
            FULL OUTER JOIN publish p
              ON p.minute_bucket = COALESCE(h.minute_bucket, i.minute_bucket)
            ORDER BY minute_bucket
        """)).mappings().all()

    out = []
    for r in rows:
        d = dict(r)
        d["head_eligible_posts"] = d["head_missing_alt_posts"] + d["head_partial_alt_posts"]
        d["intake_minus_head_image"] = d["intake_rows"] - d["head_image_posts"]
        d["apply_minus_head_eligible"] = d["intake_applied"] - d["head_eligible_posts"]
        d["skip_plus_apply_minus_intake"] = (d["intake_applied"] + d["intake_skipped"]) - d["intake_rows"]
        d["publish_jobs_minus_applied"] = d["publish_jobs_total"] - d["intake_applied"]
        d["published_minus_applied"] = d["publish_jobs_published"] - d["intake_applied"]

        d["intake_vs_head_image_ratio"] = (
            None if d["head_image_posts"] == 0 else round(d["intake_rows"] / d["head_image_posts"], 4)
        )
        d["apply_vs_head_eligible_ratio"] = (
            None if d["head_eligible_posts"] == 0 else round(d["intake_applied"] / d["head_eligible_posts"], 4)
        )
        d["published_vs_applied_ratio"] = (
            None if d["intake_applied"] == 0 else round(d["publish_jobs_published"] / d["intake_applied"], 4)
        )
        out.append(d)

    print(json.dumps({
        "lookback_minutes": lookback_minutes,
        "end_lag_minutes": end_lag_minutes,
        "rows": out,
    }, indent=2, default=str))


if __name__ == "__main__":
    main()
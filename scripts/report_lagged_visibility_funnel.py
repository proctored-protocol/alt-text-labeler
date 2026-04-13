from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from app.db import get_engine


def safe_ratio(num: int, den: int) -> float | None:
    if den <= 0:
        return None
    return round(num / den, 4)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def main() -> None:
    lookback_minutes = int(os.getenv("LOOKBACK_MINUTES", "60"))
    end_lag_minutes = int(os.getenv("END_LAG_MINUTES", "15"))

    now = utc_now()
    window_end = now - timedelta(minutes=end_lag_minutes)
    window_start = window_end - timedelta(minutes=lookback_minutes)

    with get_engine().connect() as conn:
        head = conn.execute(text("""
            SELECT
                COALESCE(SUM(post_count), 0) AS head_post_creates,
                COALESCE(SUM(image_post_count), 0) AS head_image_posts,
                COALESCE(SUM(missing_alt_post_count), 0) AS head_missing_alt_posts,
                COALESCE(SUM(partial_alt_post_count), 0) AS head_partial_alt_posts
            FROM firehose_head_sample
            WHERE bucket_second >= :window_start
              AND bucket_second < :window_end
        """), {
            "window_start": window_start,
            "window_end": window_end,
        }).mappings().one()

        intake = conn.execute(text("""
            SELECT
                COUNT(*) AS intake_rows,
                COUNT(*) FILTER (WHERE apply_status = 'applied') AS applied_rows,
                COUNT(*) FILTER (WHERE apply_status = 'skipped') AS skipped_rows,
                COUNT(*) FILTER (WHERE apply_status = 'pending') AS pending_rows,
                COUNT(*) FILTER (WHERE apply_status = 'leased') AS leased_rows
            FROM intake_item ii
            WHERE COALESCE(ii.record_created_at, ii.firehose_observed_at) >= :window_start
              AND COALESCE(ii.record_created_at, ii.firehose_observed_at) < :window_end
        """), {
            "window_start": window_start,
            "window_end": window_end,
        }).mappings().one()

        publish = conn.execute(text("""
            SELECT
                COUNT(*) FILTER (
                    WHERE ii.apply_status = 'applied'
                      AND pj.id IS NOT NULL
                ) AS publish_jobs_total,
                COUNT(*) FILTER (
                    WHERE ii.apply_status = 'applied'
                      AND pj.status = 'published'
                ) AS publish_jobs_published,
                COUNT(*) FILTER (
                    WHERE ii.apply_status = 'applied'
                      AND pj.id IS NULL
                ) AS applied_no_publish_job,
                COUNT(*) FILTER (
                    WHERE ii.apply_status = 'applied'
                      AND pj.id IS NOT NULL
                      AND pj.status <> 'published'
                ) AS publish_not_published
            FROM intake_item ii
            LEFT JOIN publish_job pj
              ON pj.uri = ii.uri
            WHERE COALESCE(ii.record_created_at, ii.firehose_observed_at) >= :window_start
              AND COALESCE(ii.record_created_at, ii.firehose_observed_at) < :window_end
        """), {
            "window_start": window_start,
            "window_end": window_end,
        }).mappings().one()

        visibility = conn.execute(text("""
            SELECT
                COUNT(*) FILTER (WHERE vc.status = 'visible') AS visible,
                COUNT(*) FILTER (WHERE vc.status = 'not_visible') AS published_not_visible,
                COUNT(*) FILTER (WHERE vc.status = 'not_found') AS baseline_not_found,
                COUNT(*) FILTER (WHERE vc.status IN ('error', 'timeout')) AS published_other_error
            FROM intake_item ii
            JOIN publish_job pj
              ON pj.uri = ii.uri
             AND ii.apply_status = 'applied'
             AND pj.status = 'published'
            LEFT JOIN visibility_check vc
              ON vc.publish_job_id = pj.id
            WHERE COALESCE(ii.record_created_at, ii.firehose_observed_at) >= :window_start
              AND COALESCE(ii.record_created_at, ii.firehose_observed_at) < :window_end
        """), {
            "window_start": window_start,
            "window_end": window_end,
        }).mappings().one()

        remediation = conn.execute(text("""
            SELECT
                COUNT(*) FILTER (WHERE vr.status = 'visible_after_first') AS visible_after_first_recovery,
                COUNT(*) FILTER (WHERE vr.status = 'visible_after_second') AS visible_after_second_recovery,
                COUNT(*) FILTER (WHERE vr.status = 'not_found') AS remediation_not_found,
                COUNT(*) FILTER (WHERE vr.status IN ('pending', 'leased', 'gave_up', 'error')) AS remediation_unresolved
            FROM intake_item ii
            JOIN publish_job pj
              ON pj.uri = ii.uri
             AND ii.apply_status = 'applied'
             AND pj.status = 'published'
            LEFT JOIN visibility_remediation vr
              ON vr.publish_job_id = pj.id
            WHERE COALESCE(ii.record_created_at, ii.firehose_observed_at) >= :window_start
              AND COALESCE(ii.record_created_at, ii.firehose_observed_at) < :window_end
        """), {
            "window_start": window_start,
            "window_end": window_end,
        }).mappings().one()

    head_post_creates = int(head["head_post_creates"] or 0)
    head_image_posts = int(head["head_image_posts"] or 0)
    head_missing_alt_posts = int(head["head_missing_alt_posts"] or 0)
    head_partial_alt_posts = int(head["head_partial_alt_posts"] or 0)
    head_eligible_posts = head_missing_alt_posts + head_partial_alt_posts

    intake_rows = int(intake["intake_rows"] or 0)
    applied_rows = int(intake["applied_rows"] or 0)
    skipped_rows = int(intake["skipped_rows"] or 0)
    pending_rows = int(intake["pending_rows"] or 0)
    leased_rows = int(intake["leased_rows"] or 0)

    publish_jobs_total = int(publish["publish_jobs_total"] or 0)
    publish_jobs_published = int(publish["publish_jobs_published"] or 0)
    applied_no_publish_job = int(publish["applied_no_publish_job"] or 0)
    publish_not_published = int(publish["publish_not_published"] or 0)

    visible = int(visibility["visible"] or 0)
    published_not_visible = int(visibility["published_not_visible"] or 0)
    baseline_not_found = int(visibility["baseline_not_found"] or 0)
    published_other_error = int(visibility["published_other_error"] or 0)

    visible_after_first_recovery = int(remediation["visible_after_first_recovery"] or 0)
    visible_after_second_recovery = int(remediation["visible_after_second_recovery"] or 0)
    remediation_not_found = int(remediation["remediation_not_found"] or 0)
    remediation_unresolved = int(remediation["remediation_unresolved"] or 0)

    total_not_found = baseline_not_found + remediation_not_found

    baseline_visible_denominator = publish_jobs_published - baseline_not_found
    cumulative_visible_denominator = publish_jobs_published - total_not_found

    visible_after_first_total = visible + visible_after_first_recovery
    visible_after_second_total = visible + visible_after_first_recovery + visible_after_second_recovery

    result = {
        "lookback_minutes": lookback_minutes,
        "end_lag_minutes": end_lag_minutes,
        "range": {
            "window_start_utc": window_start.isoformat(),
            "window_end_utc": window_end.isoformat(),
        },
        "absolute_counts": {
            "head_post_creates": head_post_creates,
            "head_image_posts": head_image_posts,
            "head_missing_alt_posts": head_missing_alt_posts,
            "head_partial_alt_posts": head_partial_alt_posts,
            "head_eligible_posts": head_eligible_posts,
            "intake_rows": intake_rows,
            "applied_rows": applied_rows,
            "skipped_rows": skipped_rows,
            "pending_rows": pending_rows,
            "leased_rows": leased_rows,
            "publish_jobs_total": publish_jobs_total,
            "publish_jobs_published": publish_jobs_published,
            "visible": visible,
            "visible_after_first_recovery": visible_after_first_recovery,
            "visible_after_second_recovery": visible_after_second_recovery,
            "visible_after_first_total": visible_after_first_total,
            "visible_after_second_total": visible_after_second_total,
            "published_not_visible": published_not_visible,
            "published_other_error": published_other_error,
            "baseline_not_found": baseline_not_found,
            "remediation_not_found": remediation_not_found,
            "total_not_found": total_not_found,
            "applied_no_publish_job": applied_no_publish_job,
            "publish_not_published": publish_not_published,
            "remediation_unresolved": remediation_unresolved,
        },
        "ratios": {
            "image_posts_vs_post_creates": safe_ratio(head_image_posts, head_post_creates),
            "eligible_vs_image_posts": safe_ratio(head_eligible_posts, head_image_posts),
            "intake_vs_head_image": safe_ratio(intake_rows, head_image_posts),
            "applied_vs_head_eligible": safe_ratio(applied_rows, head_eligible_posts),
            "publish_jobs_vs_applied": safe_ratio(publish_jobs_total, applied_rows),
            "published_vs_applied": safe_ratio(publish_jobs_published, applied_rows),
            "visible_vs_published_excluding_not_found": safe_ratio(visible, baseline_visible_denominator),
            "visible_vs_head_eligible": safe_ratio(visible, head_eligible_posts),
            "failed_to_get_visible_label_within_5m_vs_head_eligible": safe_ratio(
                applied_no_publish_job
                + publish_not_published
                + published_not_visible
                + published_other_error,
                head_eligible_posts,
            ),
            "not_found_vs_head_eligible": safe_ratio(total_not_found, head_eligible_posts),
            "visible_after_first_remediation": safe_ratio(
                visible_after_first_total,
                cumulative_visible_denominator,
            ),
            "visible_after_second_remediation": safe_ratio(
                visible_after_second_total,
                cumulative_visible_denominator,
            ),
        },
        "decomposition": {
            "pre_publish_gap": max(head_eligible_posts - applied_rows, 0),
            "applied_no_publish_job": applied_no_publish_job,
            "publish_not_published": publish_not_published,
            "published_not_visible": published_not_visible,
            "published_other_error": published_other_error,
            "baseline_not_found": baseline_not_found,
            "visible_after_first_recovery": visible_after_first_recovery,
            "visible_after_second_recovery": visible_after_second_recovery,
            "remediation_not_found": remediation_not_found,
            "remediation_unresolved": remediation_unresolved,
        },
    }

    print(json.dumps(result, indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
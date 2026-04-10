from __future__ import annotations

import json
import os
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import text

from app.config import get_settings
from app.db import get_engine
from app.visibility.client import VisibilityClient, VisibilityClientError


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def latest_head_bucket_utc() -> datetime:
    with get_engine().connect() as conn:
        row = conn.execute(text("""
            SELECT bucket_second
            FROM firehose_head_sample
            ORDER BY bucket_second DESC
            LIMIT 1
        """)).mappings().one_or_none()

    if row is None:
        raise RuntimeError("No firehose_head_sample rows found.")

    return row["bucket_second"].astimezone(timezone.utc)


def load_head_counts(start_utc: datetime, end_utc: datetime) -> dict[str, int]:
    with get_engine().connect() as conn:
        row = conn.execute(text("""
            SELECT
                COALESCE(SUM(post_count), 0) AS head_post_creates,
                COALESCE(SUM(image_post_count), 0) AS head_image_posts,
                COALESCE(SUM(missing_alt_post_count), 0) AS head_missing_alt_posts,
                COALESCE(SUM(partial_alt_post_count), 0) AS head_partial_alt_posts
            FROM firehose_head_sample
            WHERE bucket_second >= :start_utc
              AND bucket_second < :end_utc
        """), {"start_utc": start_utc, "end_utc": end_utc}).mappings().one()

    return {
        "head_post_creates": int(row["head_post_creates"]),
        "head_image_posts": int(row["head_image_posts"]),
        "head_missing_alt_posts": int(row["head_missing_alt_posts"]),
        "head_partial_alt_posts": int(row["head_partial_alt_posts"]),
        "head_eligible_posts": int(row["head_missing_alt_posts"]) + int(row["head_partial_alt_posts"]),
    }


def load_cohort_rows(start_utc: datetime, end_utc: datetime) -> list[dict[str, Any]]:
    with get_engine().connect() as conn:
        rows = conn.execute(text("""
            SELECT
                ii.id,
                ii.uri,
                ii.author_did,
                ii.repo_did,
                ii.raw_embed_type,
                ii.image_count,
                ii.image_alts_json,
                ii.apply_status,
                COALESCE(ii.record_created_at, ii.firehose_observed_at) AS cohort_time,
                ii.firehose_observed_at,
                ii.firehose_seq,
                pj.id AS publish_job_id,
                pj.status AS publish_status,
                pj.label_value,
                pj.published_at
            FROM intake_item ii
            LEFT JOIN publish_job pj
              ON pj.uri = ii.uri
            WHERE COALESCE(ii.record_created_at, ii.firehose_observed_at) >= :start_utc
              AND COALESCE(ii.record_created_at, ii.firehose_observed_at) < :end_utc
            ORDER BY COALESCE(ii.record_created_at, ii.firehose_observed_at) ASC, ii.id ASC
        """), {"start_utc": start_utc, "end_utc": end_utc}).mappings().all()

    return [dict(r) for r in rows]


def make_visibility_client() -> VisibilityClient:
    settings = get_settings()
    return VisibilityClient(
        pds_url=settings.bsky_pds_url,
        appview_url=settings.verifier_appview_url,
        viewer_identifier=settings.test_viewer_handle,
        viewer_password=settings.test_viewer_app_password,
        labeler_did=settings.verifier_labeler_did,
        timeout_seconds=settings.visibility_request_timeout_seconds,
    )


def classify_and_check(rows: list[dict[str, Any]]) -> dict[str, Any]:
    client = make_visibility_client()

    intake_rows = len(rows)
    applied_rows = sum(1 for r in rows if r["apply_status"] == "applied")
    skipped_rows = sum(1 for r in rows if r["apply_status"] == "skipped")
    pending_rows = sum(1 for r in rows if r["apply_status"] == "pending")
    leased_rows = sum(1 for r in rows if r["apply_status"] == "leased")

    applied_no_publish_job = 0
    publish_not_published = 0
    publish_jobs_total = 0
    publish_jobs_published = 0

    visible = 0
    not_visible = 0
    not_found = 0
    other_error_total = 0
    other_errors: dict[str, int] = {}

    failure_examples: list[dict[str, Any]] = []
    author_counter = Counter()
    embed_counter = Counter()
    image_count_counter = Counter()
    label_value_counter = Counter()

    for row in rows:
        if row["apply_status"] != "applied":
            continue

        base = {
            "uri": row["uri"],
            "author_did": row["author_did"],
            "repo_did": row["repo_did"],
            "cohort_time": str(row["cohort_time"]),
            "firehose_seq": row["firehose_seq"],
            "raw_embed_type": row["raw_embed_type"],
            "image_count": row["image_count"],
            "label_value": row["label_value"],
            "publish_job_id": row["publish_job_id"],
            "publish_status": row["publish_status"],
            "published_at": str(row["published_at"]),
        }

        if row["publish_job_id"] is None:
            applied_no_publish_job += 1
            author_counter[row["author_did"]] += 1
            embed_counter[str(row["raw_embed_type"])] += 1
            image_count_counter[int(row["image_count"] or 0)] += 1
            label_value_counter[str(row["label_value"])] += 1
            if len(failure_examples) < 25:
                failure_examples.append({**base, "failure_class": "applied_no_publish_job"})
            continue

        publish_jobs_total += 1

        if row["publish_status"] != "published":
            publish_not_published += 1
            author_counter[row["author_did"]] += 1
            embed_counter[str(row["raw_embed_type"])] += 1
            image_count_counter[int(row["image_count"] or 0)] += 1
            label_value_counter[str(row["label_value"])] += 1
            if len(failure_examples) < 25:
                failure_examples.append({**base, "failure_class": "publish_not_published"})
            continue

        publish_jobs_published += 1

        try:
            result = client.check_forced_hydration(
                uri=row["uri"],
                label_value=row["label_value"],
            )
            if result.found_label:
                visible += 1
            else:
                not_visible += 1
                author_counter[row["author_did"]] += 1
                embed_counter[str(row["raw_embed_type"])] += 1
                image_count_counter[int(row["image_count"] or 0)] += 1
                label_value_counter[str(row["label_value"])] += 1
                if len(failure_examples) < 25:
                    failure_examples.append({**base, "failure_class": "published_not_visible"})
        except VisibilityClientError as exc:
            if exc.http_status == 400 and exc.error_code == "NotFound":
                not_found += 1
                author_counter[row["author_did"]] += 1
                embed_counter[str(row["raw_embed_type"])] += 1
                image_count_counter[int(row["image_count"] or 0)] += 1
                label_value_counter[str(row["label_value"])] += 1
                if len(failure_examples) < 25:
                    failure_examples.append({
                        **base,
                        "failure_class": "published_not_found",
                        "http_status": exc.http_status,
                        "error_code": exc.error_code,
                        "error_text": exc.error_text,
                    })
            else:
                other_error_total += 1
                key = f"{exc.http_status}:{exc.error_code}"
                other_errors[key] = other_errors.get(key, 0) + 1
                author_counter[row["author_did"]] += 1
                embed_counter[str(row["raw_embed_type"])] += 1
                image_count_counter[int(row["image_count"] or 0)] += 1
                label_value_counter[str(row["label_value"])] += 1
                if len(failure_examples) < 25:
                    failure_examples.append({
                        **base,
                        "failure_class": "published_other_error",
                        "http_status": exc.http_status,
                        "error_code": exc.error_code,
                        "error_text": exc.error_text,
                    })

    visible_ratio_excluding_not_found = None
    visible_denominator = publish_jobs_published - not_found
    if visible_denominator > 0:
        visible_ratio_excluding_not_found = round(visible / visible_denominator, 4)

    return {
        "pipeline_counts": {
            "intake_rows": intake_rows,
            "applied_rows": applied_rows,
            "skipped_rows": skipped_rows,
            "pending_rows": pending_rows,
            "leased_rows": leased_rows,
            "publish_jobs_total": publish_jobs_total,
            "publish_jobs_published": publish_jobs_published,
            "visible": visible,
            "not_visible": not_visible,
            "not_found": not_found,
            "other_error_total": other_error_total,
        },
        "failure_classes": {
            "applied_no_publish_job": applied_no_publish_job,
            "publish_not_published": publish_not_published,
            "published_not_visible": not_visible,
            "published_not_found": not_found,
            "published_other_error": other_error_total,
        },
        "other_errors": other_errors,
        "visible_ratio_excluding_not_found": visible_ratio_excluding_not_found,
        "failure_pattern_summary": {
            "top_authors": author_counter.most_common(20),
            "top_embed_types": embed_counter.most_common(20),
            "top_image_counts": image_count_counter.most_common(10),
            "top_label_values": label_value_counter.most_common(10),
        },
        "failure_examples": failure_examples,
    }


def evaluate_cohort(cohort_start_utc: datetime, cohort_end_utc: datetime) -> dict[str, Any]:
    head = load_head_counts(cohort_start_utc, cohort_end_utc)
    rows = load_cohort_rows(cohort_start_utc, cohort_end_utc)
    checked = classify_and_check(rows)

    pipeline_counts = checked["pipeline_counts"]
    failure_classes = checked["failure_classes"]

    result = {
        "generated_at_utc": utc_now().isoformat(),
        "cohort_start_utc": cohort_start_utc.isoformat(),
        "cohort_end_utc": cohort_end_utc.isoformat(),
        **head,
        **pipeline_counts,
        **failure_classes,
        "visible_ratio_excluding_not_found": checked["visible_ratio_excluding_not_found"],
        "reconciliation": {
            "intake_minus_head_image": pipeline_counts["intake_rows"] - head["head_image_posts"],
            "apply_minus_head_eligible": pipeline_counts["applied_rows"] - head["head_eligible_posts"],
            "publish_jobs_minus_applied": pipeline_counts["publish_jobs_total"] - pipeline_counts["applied_rows"],
            "published_minus_applied": pipeline_counts["publish_jobs_published"] - pipeline_counts["applied_rows"],
            "visible_minus_head_eligible": pipeline_counts["visible"] - head["head_eligible_posts"],
        },
        "failure_pattern_summary": checked["failure_pattern_summary"],
        "failure_examples": checked["failure_examples"],
        "other_errors": checked["other_errors"],
    }
    return result


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    line = json.dumps(payload, ensure_ascii=False, default=str)
    print(line, flush=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def main() -> None:
    mode = os.getenv("MODE", "watch").strip().lower()
    cohort_lag_minutes = int(os.getenv("COHORT_LAG_MINUTES", "5"))
    cohort_width_minutes = int(os.getenv("COHORT_WIDTH_MINUTES", "1"))
    poll_seconds = int(os.getenv("POLL_SECONDS", "60"))
    backfill_cohorts = int(os.getenv("BACKFILL_COHORTS", "0"))

    metrics_dir = Path("metrics")
    metrics_dir.mkdir(exist_ok=True)
    output_path = Path(os.getenv(
        "OUTPUT_PATH",
        metrics_dir / f"lagged_visibility_monitor_{utc_now().strftime('%Y%m%dT%H%M%SZ')}.jsonl",
    ))

    if mode == "once":
        latest_head = latest_head_bucket_utc()
        cohort_end = latest_head - timedelta(minutes=cohort_lag_minutes)
        cohort_start = cohort_end - timedelta(minutes=cohort_width_minutes)
        payload = evaluate_cohort(cohort_start, cohort_end)
        print(json.dumps(payload, indent=2, ensure_ascii=False, default=str))
        return

    if mode == "backfill":
        latest_head = latest_head_bucket_utc()
        out = []
        for offset in range(backfill_cohorts, 0, -1):
            cohort_end = latest_head - timedelta(minutes=cohort_lag_minutes + (offset - 1))
            cohort_start = cohort_end - timedelta(minutes=cohort_width_minutes)
            out.append(evaluate_cohort(cohort_start, cohort_end))
        print(json.dumps(out, indent=2, ensure_ascii=False, default=str))
        return

    while True:
        latest_head = latest_head_bucket_utc()
        cohort_end = latest_head - timedelta(minutes=cohort_lag_minutes)
        cohort_start = cohort_end - timedelta(minutes=cohort_width_minutes)
        payload = evaluate_cohort(cohort_start, cohort_end)
        append_jsonl(output_path, payload)
        time.sleep(poll_seconds)


if __name__ == "__main__":
    main()
from __future__ import annotations

import json
import os
from collections import Counter
from datetime import datetime, timezone

from sqlalchemy import text

from app.config import get_settings
from app.db import get_engine
from app.visibility.client import VisibilityClient, VisibilityClientError


def parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def make_client() -> VisibilityClient:
    s = get_settings()
    return VisibilityClient(
        pds_url=s.bsky_pds_url,
        appview_url=s.verifier_appview_url,
        viewer_identifier=s.test_viewer_handle,
        viewer_password=s.test_viewer_app_password,
        labeler_did=s.verifier_labeler_did,
        timeout_seconds=s.visibility_request_timeout_seconds,
    )


def main() -> None:
    start_utc = parse_dt(os.environ["COHORT_START_UTC"])
    end_utc = parse_dt(os.environ["COHORT_END_UTC"])
    limit_examples = int(os.getenv("LIMIT_EXAMPLES", "200"))

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
              AND ii.apply_status = 'applied'
            ORDER BY COALESCE(ii.record_created_at, ii.firehose_observed_at) ASC, ii.id ASC
        """), {"start_utc": start_utc, "end_utc": end_utc}).mappings().all()

    client = make_client()

    failures = {
        "applied_no_publish_job": [],
        "publish_not_published": [],
        "published_not_visible": [],
        "published_not_found": [],
        "published_other_error": [],
    }

    author_counter = Counter()
    embed_counter = Counter()
    image_count_counter = Counter()
    label_value_counter = Counter()

    for row in rows:
        base = {
            "uri": row["uri"],
            "author_did": row["author_did"],
            "repo_did": row["repo_did"],
            "cohort_time": str(row["cohort_time"]),
            "firehose_observed_at": str(row["firehose_observed_at"]),
            "firehose_seq": row["firehose_seq"],
            "raw_embed_type": row["raw_embed_type"],
            "image_count": row["image_count"],
            "image_alts_json": row["image_alts_json"],
            "publish_job_id": row["publish_job_id"],
            "publish_status": row["publish_status"],
            "label_value": row["label_value"],
            "published_at": str(row["published_at"]),
        }

        failure_class = None

        if row["publish_job_id"] is None:
            failure_class = "applied_no_publish_job"
        elif row["publish_status"] != "published":
            failure_class = "publish_not_published"
        else:
            try:
                result = client.check_forced_hydration(
                    uri=row["uri"],
                    label_value=row["label_value"],
                )
                if not result.found_label:
                    failure_class = "published_not_visible"
            except VisibilityClientError as exc:
                if exc.http_status == 400 and exc.error_code == "NotFound":
                    failure_class = "published_not_found"
                    base["http_status"] = exc.http_status
                    base["error_code"] = exc.error_code
                    base["error_text"] = exc.error_text
                else:
                    failure_class = "published_other_error"
                    base["http_status"] = exc.http_status
                    base["error_code"] = exc.error_code
                    base["error_text"] = exc.error_text

        if failure_class is None:
            continue

        author_counter[row["author_did"]] += 1
        embed_counter[str(row["raw_embed_type"])] += 1
        image_count_counter[int(row["image_count"] or 0)] += 1
        label_value_counter[str(row["label_value"])] += 1

        if len(failures[failure_class]) < limit_examples:
            failures[failure_class].append(base)

    result = {
        "cohort_start_utc": start_utc.isoformat(),
        "cohort_end_utc": end_utc.isoformat(),
        "failure_counts": {k: len(v) for k, v in failures.items()},
        "pattern_summary": {
            "top_authors": author_counter.most_common(50),
            "top_embed_types": embed_counter.most_common(20),
            "top_image_counts": image_count_counter.most_common(10),
            "top_label_values": label_value_counter.most_common(10),
        },
        "examples": failures,
    }

    print(json.dumps(result, indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
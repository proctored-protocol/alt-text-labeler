from __future__ import annotations

import json
import os
from datetime import datetime, timezone

from sqlalchemy import text

from app.config import get_settings
from app.db import get_engine
from app.visibility.client import VisibilityClient, VisibilityClientError


def parse_utc(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)


def main() -> None:
    start_utc = parse_utc(os.environ["COHORT_START_UTC"])
    end_utc = parse_utc(os.environ["COHORT_END_UTC"])

    settings = get_settings()
    client = VisibilityClient(
        pds_url=settings.bsky_pds_url,
        appview_url=settings.verifier_appview_url,
        viewer_identifier=settings.test_viewer_handle,
        viewer_password=settings.test_viewer_app_password,
        labeler_did=settings.verifier_labeler_did,
        timeout_seconds=settings.visibility_request_timeout_seconds,
    )

    with get_engine().connect() as conn:
        rows = conn.execute(text("""
            SELECT
                ii.id,
                ii.uri,
                ii.author_did,
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
        """), {
            "start_utc": start_utc,
            "end_utc": end_utc,
        }).mappings().all()

    out = {
        "cohort_start_utc": start_utc.isoformat(),
        "cohort_end_utc": end_utc.isoformat(),
        "applied_no_publish_job": [],
        "publish_not_published": [],
        "published_not_visible": [],
        "published_not_found": [],
        "published_other_error": [],
    }

    for row in rows:
        base = {
            "uri": row["uri"],
            "author_did": row["author_did"],
            "cohort_time": str(row["cohort_time"]),
            "firehose_seq": row["firehose_seq"],
            "raw_embed_type": row["raw_embed_type"],
            "image_count": row["image_count"],
            "image_alts_json": row["image_alts_json"],
            "publish_job_id": row["publish_job_id"],
            "publish_status": row["publish_status"],
            "label_value": row["label_value"],
            "published_at": str(row["published_at"]),
        }

        if row["publish_job_id"] is None:
            out["applied_no_publish_job"].append(base)
            continue

        if row["publish_status"] != "published":
            out["publish_not_published"].append(base)
            continue

        try:
            result = client.check_forced_hydration(
                uri=row["uri"],
                label_value=row["label_value"],
            )
            if not result.found_label:
                out["published_not_visible"].append(base)
        except VisibilityClientError as exc:
            enriched = dict(base)
            enriched["http_status"] = exc.http_status
            enriched["error_code"] = exc.error_code
            enriched["error_text"] = exc.error_text

            if exc.http_status == 400 and exc.error_code == "NotFound":
                out["published_not_found"].append(enriched)
            else:
                out["published_other_error"].append(enriched)

    print(json.dumps(out, indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
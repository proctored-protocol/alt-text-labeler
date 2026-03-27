from __future__ import annotations

import argparse
import json
import re
import subprocess
from collections import OrderedDict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import urlopen

from sqlalchemy import text

from app.config import get_settings
from app.db import engine


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def normalize(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    return value


def safe_run_ps() -> list[str]:
    try:
        completed = subprocess.run(
            ["ps", "-eo", "pid,ppid,cmd"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
            timeout=10,
        )
        if completed.returncode != 0:
            return []
        return completed.stdout.splitlines()
    except Exception:
        return []


def collect_process_info() -> dict[str, Any]:
    lines = safe_run_ps()

    firehose = []
    enqueue = []
    apply = []
    verify = []

    worker_id_pattern = re.compile(r"--worker-id\s+(\S+)")

    for line in lines:
        if "scripts/run_intake_worker.py" in line:
            firehose.append(line.strip())
        elif "scripts/run_candidate_enqueue_worker.py" in line:
            enqueue.append(line.strip())
        elif "scripts/run_label_apply_worker.py" in line:
            worker_id = None
            match = worker_id_pattern.search(line)
            if match:
                worker_id = match.group(1)
            apply.append({"line": line.strip(), "worker_id": worker_id})
        elif "scripts/run_label_verify_worker.py" in line:
            worker_id = None
            match = worker_id_pattern.search(line)
            if match:
                worker_id = match.group(1)
            verify.append({"line": line.strip(), "worker_id": worker_id})

    return {
        "firehose_intake_count": len(firehose),
        "candidate_enqueue_count": len(enqueue),
        "label_apply_count": len(apply),
        "label_apply_worker_ids": sorted(
            [item["worker_id"] for item in apply if item["worker_id"]]
        ),
        "label_verify_count": len(verify),
        "label_verify_worker_ids": sorted(
            [item["worker_id"] for item in verify if item["worker_id"]]
        ),
    }


def fetch_window_metrics(conn, window_minutes: int) -> dict[str, Any]:
    row = conn.execute(
        text(
            f"""
            SELECT
                (
                    SELECT COUNT(*)
                    FROM post_evaluation pe
                    WHERE pe.evaluated_at >= NOW() - INTERVAL '{window_minutes} minutes'
                ) AS post_evaluation_rows,

                (
                    SELECT COUNT(*)
                    FROM post_evaluation pe
                    WHERE pe.evaluated_at >= NOW() - INTERVAL '{window_minutes} minutes'
                      AND pe.derived_label IN ('missing-alt-text', 'partial-alt-text')
                ) AS labeled_rows,

                (
                    SELECT COUNT(*)
                    FROM label_work_item lwi
                    WHERE lwi.created_at >= NOW() - INTERVAL '{window_minutes} minutes'
                ) AS queued_rows,

                (
                    SELECT COUNT(*)
                    FROM label_work_item lwi
                    WHERE lwi.ozone_created_at >= NOW() - INTERVAL '{window_minutes} minutes'
                ) AS emitted_rows,

                (
                    SELECT COUNT(*)
                    FROM label_work_item lwi
                    WHERE lwi.label_visible_at >= NOW() - INTERVAL '{window_minutes} minutes'
                ) AS verified_rows,

                (
                    SELECT COUNT(*)
                    FROM label_work_item lwi
                    WHERE lwi.state = 'verification_failed'
                      AND lwi.updated_at >= NOW() - INTERVAL '{window_minutes} minutes'
                ) AS verification_failed_rows,

                (
                    SELECT COUNT(*)
                    FROM label_work_item lwi
                    WHERE lwi.state = 'dead'
                      AND lwi.updated_at >= NOW() - INTERVAL '{window_minutes} minutes'
                ) AS dead_rows
            """
        )
    ).mappings().one()

    return {k: normalize(v) for k, v in dict(row).items()}


def fetch_current_counts(conn) -> dict[str, Any]:
    row = conn.execute(
        text(
            """
            SELECT
                COUNT(*) FILTER (WHERE state = 'queued') AS queued_count,
                COUNT(*) FILTER (WHERE state = 'leased') AS leased_count,
                COUNT(*) FILTER (WHERE state = 'published_pending_verification') AS pending_verification_count,
                COUNT(*) FILTER (WHERE state = 'verifying') AS verifying_count,
                COUNT(*) FILTER (WHERE state = 'published') AS published_count,
                COUNT(*) FILTER (WHERE state = 'verification_failed') AS verification_failed_count,
                COUNT(*) FILTER (WHERE state = 'dead') AS dead_count
            FROM label_work_item
            """
        )
    ).mappings().one()
    return {k: normalize(v) for k, v in dict(row).items()}


def fetch_fresh_cohorts(conn) -> dict[str, Any]:
    rows = conn.execute(
        text(
            """
            WITH base AS (
                SELECT
                    state,
                    ozone_created_at,
                    label_visible_at,
                    final_forced_found_label,
                    final_query_found_label,
                    EXTRACT(EPOCH FROM (NOW() - ozone_created_at)) AS age_seconds
                FROM label_work_item
                WHERE ozone_created_at IS NOT NULL
                  AND ozone_created_at >= NOW() - INTERVAL '24 hours'
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
                    *
                FROM base
            )
            SELECT
                bucket,
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
                ) AS any_visible_count
            FROM bucketed
            GROUP BY bucket
            """
        )
    ).mappings().all()

    order = ["0-2m", "2-10m", "10-30m", "30-60m", "60m+"]
    out: OrderedDict[str, dict[str, Any]] = OrderedDict()
    by_bucket = {row["bucket"]: dict(row) for row in rows}

    for bucket in order:
        raw = by_bucket.get(bucket, {"bucket": bucket})
        item = {k: normalize(v) for k, v in raw.items()}
        emitted = item.get("emitted_count", 0) or 0
        forced = item.get("forced_visible_count", 0) or 0
        query = item.get("query_visible_count", 0) or 0
        any_visible = item.get("any_visible_count", 0) or 0
        item["forced_visible_pct"] = round((forced / emitted) * 100, 2) if emitted else None
        item["query_visible_pct"] = round((query / emitted) * 100, 2) if emitted else None
        item["any_visible_pct"] = round((any_visible / emitted) * 100, 2) if emitted else None
        out[bucket] = item

    return out


def fetch_pending_age_buckets(conn) -> dict[str, Any]:
    rows = conn.execute(
        text(
            """
            WITH base AS (
                SELECT
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
                    *
                FROM base
            )
            SELECT
                bucket,
                COUNT(*) AS total_count,
                COUNT(*) FILTER (WHERE state = 'published_pending_verification') AS pending_count,
                COUNT(*) FILTER (WHERE state = 'verifying') AS verifying_count,
                COUNT(*) FILTER (WHERE final_forced_found_label IS TRUE) AS forced_visible_partial_count,
                COUNT(*) FILTER (WHERE final_query_found_label IS TRUE) AS query_visible_partial_count,
                COUNT(*) FILTER (WHERE last_error IS NOT NULL AND last_error <> '') AS rows_with_last_error
            FROM bucketed
            GROUP BY bucket
            """
        )
    ).mappings().all()

    order = ["0-2m", "2-10m", "10-30m", "30-60m", "1-4h", "4h+"]
    out: OrderedDict[str, dict[str, Any]] = OrderedDict()
    by_bucket = {row["bucket"]: dict(row) for row in rows}

    for bucket in order:
        raw = by_bucket.get(bucket, {"bucket": bucket})
        out[bucket] = {k: normalize(v) for k, v in raw.items()}

    return out


def fetch_cursor_state(conn) -> dict[str, Any]:
    row = conn.execute(
        text(
            """
            SELECT
                (
                    SELECT fc.last_seq
                    FROM firehose_cursor fc
                    WHERE fc.stream_name = 'subscribe_repos'
                    LIMIT 1
                ) AS firehose_cursor_last_seq,
                (
                    SELECT fc.updated_at
                    FROM firehose_cursor fc
                    WHERE fc.stream_name = 'subscribe_repos'
                    LIMIT 1
                ) AS firehose_cursor_updated_at,
                (
                    SELECT MAX(pe.last_seen_seq)
                    FROM post_evaluation pe
                ) AS max_last_seen_seq,
                (
                    SELECT MAX(pe.evaluated_at)
                    FROM post_evaluation pe
                ) AS max_evaluated_at
            """
        )
    ).mappings().one()
    return {k: normalize(v) for k, v in dict(row).items()}


def fetch_service_record_summary() -> dict[str, Any]:
    try:
        settings = get_settings()
        did = settings.ozone_proxy_did.split("#", 1)[0]
        url = "https://bsky.social/xrpc/com.atproto.repo.getRecord?" + urlencode(
            {
                "repo": did,
                "collection": "app.bsky.labeler.service",
                "rkey": "self",
            }
        )
        with urlopen(url, timeout=30) as resp:
            payload = json.loads(resp.read().decode("utf-8"))

        value = payload.get("value") or {}
        policies = value.get("policies") or {}

        return {
            "ok": True,
            "uri": payload.get("uri"),
            "cid": payload.get("cid"),
            "createdAt": value.get("createdAt"),
            "labelValues": policies.get("labelValues"),
            "subjectTypes": value.get("subjectTypes"),
            "subjectCollections": value.get("subjectCollections"),
        }
    except Exception as exc:
        return {
            "ok": False,
            "error": str(exc),
        }


def build_snapshot(include_service_record: bool) -> dict[str, Any]:
    with engine.connect() as conn:
        snapshot = {
            "generated_at_utc": utc_now_iso(),
            "process_health": collect_process_info(),
            "current_counts": fetch_current_counts(conn),
            "window_10m": fetch_window_metrics(conn, 10),
            "window_60m": fetch_window_metrics(conn, 60),
            "fresh_cohorts": fetch_fresh_cohorts(conn),
            "pending_age_buckets": fetch_pending_age_buckets(conn),
            "cursor_state": fetch_cursor_state(conn),
        }

    if include_service_record:
        snapshot["service_record"] = fetch_service_record_summary()

    return snapshot


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False, default=str))
        f.write("\n")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Collect one pipeline metrics snapshot and optionally append it to JSONL."
    )
    parser.add_argument(
        "--output-jsonl",
        default="metrics/pipeline_timeseries.jsonl",
        help="Path to append JSONL snapshots to.",
    )
    parser.add_argument(
        "--no-append",
        action="store_true",
        help="Do not append to JSONL, just print the snapshot.",
    )
    parser.add_argument(
        "--no-service-record",
        action="store_true",
        help="Skip fetching the live labeler service record summary.",
    )
    args = parser.parse_args()

    snapshot = build_snapshot(include_service_record=not args.no_service_record)

    if not args.no_append:
        append_jsonl(Path(args.output_jsonl), snapshot)

    print(json.dumps(snapshot, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
from __future__ import annotations

import json
import re
import subprocess
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text

from app.db import engine


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def print_section(title: str) -> None:
    print()
    print(f"=== {title} ===")


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
            apply.append(
                {
                    "line": line.strip(),
                    "worker_id": worker_id,
                }
            )
        elif "scripts/run_label_verify_worker.py" in line:
            worker_id = None
            match = worker_id_pattern.search(line)
            if match:
                worker_id = match.group(1)
            verify.append(
                {
                    "line": line.strip(),
                    "worker_id": worker_id,
                }
            )

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


def main() -> None:
    process_info = collect_process_info()

    with engine.connect() as conn:
        lwi_counts = conn.execute(
            text(
                """
                SELECT state, COUNT(*) AS n
                FROM label_work_item
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
                    ) AS dead_last_10m,

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

        throughput_60m = conn.execute(
            text(
                """
                SELECT
                    COUNT(*) FILTER (
                        WHERE created_at >= NOW() - INTERVAL '60 minutes'
                    ) AS queued_last_60m,

                    COUNT(*) FILTER (
                        WHERE ozone_created_at >= NOW() - INTERVAL '60 minutes'
                    ) AS emitted_last_60m,

                    COUNT(*) FILTER (
                        WHERE label_visible_at >= NOW() - INTERVAL '60 minutes'
                    ) AS verified_last_60m,

                    COUNT(*) FILTER (
                        WHERE state = 'verification_failed'
                          AND updated_at >= NOW() - INTERVAL '60 minutes'
                    ) AS verification_failed_last_60m,

                    COUNT(*) FILTER (
                        WHERE state = 'dead'
                          AND updated_at >= NOW() - INTERVAL '60 minutes'
                    ) AS dead_last_60m,

                    COUNT(*) FILTER (
                        WHERE final_forced_found_label IS TRUE
                          AND label_visible_at >= NOW() - INTERVAL '60 minutes'
                    ) AS forced_visible_last_60m,

                    COUNT(*) FILTER (
                        WHERE final_query_found_label IS TRUE
                          AND label_visible_at >= NOW() - INTERVAL '60 minutes'
                    ) AS query_visible_last_60m
                FROM label_work_item
                """
            )
        ).mappings().one()

        leased_by_worker = conn.execute(
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

        firehose_state = conn.execute(
            text(
                """
                SELECT
                    fc.last_seq AS firehose_cursor_last_seq,
                    fc.updated_at AS firehose_cursor_updated_at,
                    pe.max_last_seen_seq,
                    pe.max_evaluated_at,
                    pe.recent_post_evaluation_rows,
                    pe.recent_labeled_rows
                FROM (
                    SELECT
                        MAX(last_seq) AS last_seq,
                        MAX(updated_at) AS updated_at
                    FROM firehose_cursor
                    WHERE stream_name = 'subscribe_repos'
                ) fc
                CROSS JOIN (
                    SELECT
                        MAX(last_seen_seq) AS max_last_seen_seq,
                        MAX(evaluated_at) AS max_evaluated_at,
                        COUNT(*) FILTER (
                            WHERE evaluated_at >= NOW() - INTERVAL '10 minutes'
                        ) AS recent_post_evaluation_rows,
                        COUNT(*) FILTER (
                            WHERE evaluated_at >= NOW() - INTERVAL '10 minutes'
                              AND derived_label IN ('missing-alt-text', 'partial-alt-text')
                        ) AS recent_labeled_rows
                    FROM post_evaluation
                ) pe
                """
            )
        ).mappings().one()

        backlog_shape = conn.execute(
            text(
                """
                SELECT
                    MIN(record_created_at) FILTER (WHERE state = 'queued') AS oldest_queued_record_created_at,
                    MAX(record_created_at) FILTER (WHERE state = 'queued') AS newest_queued_record_created_at,
                    MAX(record_created_at) FILTER (WHERE state = 'published') AS newest_verified_record_created_at,
                    MAX(record_created_at) FILTER (WHERE state = 'published_pending_verification') AS newest_pending_verification_record_created_at,
                    COUNT(*) FILTER (WHERE state = 'queued') AS queued_count,
                    COUNT(*) FILTER (WHERE state = 'published_pending_verification') AS pending_verification_count,
                    COUNT(*) FILTER (WHERE state = 'verifying') AS verifying_count,
                    COUNT(*) FILTER (WHERE state = 'verification_failed') AS verification_failed_count,
                    COUNT(*) FILTER (WHERE state = 'published') AS verified_count
                FROM label_work_item
                """
            )
        ).mappings().one()

        recent_problem_rows = conn.execute(
            text(
                """
                SELECT
                    id,
                    post_url,
                    state,
                    label_value,
                    last_error,
                    updated_at
                FROM label_work_item
                WHERE state IN ('dead', 'verification_failed')
                   OR (last_error IS NOT NULL AND last_error <> '')
                ORDER BY updated_at DESC
                LIMIT 10
                """
            )
        ).mappings().all()

        recent_rows = conn.execute(
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
                    updated_at
                FROM label_work_item
                ORDER BY updated_at DESC
                LIMIT 15
                """
            )
        ).mappings().all()

    print(f"generated_at_utc: {utc_now().isoformat()}")

    print_section("process health")
    print(json.dumps(process_info, ensure_ascii=False, default=str, indent=2))

    print_section("label_work_item counts")
    for row in lwi_counts:
        print(dict(row))

    print_section("10-minute throughput")
    print(dict(throughput_10m))

    print_section("60-minute throughput")
    print(dict(throughput_60m))

    print_section("leased jobs by worker")
    if leased_by_worker:
        for row in leased_by_worker:
            print(dict(row))
    else:
        print("(none)")

    print_section("firehose / evaluation state")
    print(dict(firehose_state))

    print_section("backlog shape")
    print(dict(backlog_shape))

    print_section("recent problems")
    if recent_problem_rows:
        for row in recent_problem_rows:
            item = dict(row)
            if item.get("last_error") and len(item["last_error"]) > 300:
                item["last_error"] = item["last_error"][:300] + "...[truncated]"
            print(json.dumps(item, ensure_ascii=False, default=str))
    else:
        print("(none)")

    print_section("recent label_work_item rows")
    for row in recent_rows:
        print(json.dumps(dict(row), ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
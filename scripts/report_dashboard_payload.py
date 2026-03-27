from __future__ import annotations

import argparse
import json
from collections import deque
from pathlib import Path
from typing import Any


def load_recent_snapshots(path: Path, max_points: int) -> list[dict[str, Any]]:
    items: deque[dict[str, Any]] = deque(maxlen=max_points)

    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            items.append(json.loads(line))

    return list(items)


def build_series_point(snapshot: dict[str, Any]) -> dict[str, Any]:
    current = snapshot.get("current_counts") or {}
    window_10m = snapshot.get("window_10m") or {}
    fresh = snapshot.get("fresh_cohorts") or {}
    pending = snapshot.get("pending_age_buckets") or {}
    process = snapshot.get("process_health") or {}
    service_record = snapshot.get("service_record") or {}

    fresh_2_10m = fresh.get("2-10m") or {}
    fresh_10_30m = fresh.get("10-30m") or {}

    pending_0_2m = pending.get("0-2m") or {}
    pending_2_10m = pending.get("2-10m") or {}
    pending_10_30m = pending.get("10-30m") or {}
    pending_30_60m = pending.get("30-60m") or {}
    pending_1_4h = pending.get("1-4h") or {}
    pending_4h = pending.get("4h+") or {}

    return {
        "ts": snapshot.get("generated_at_utc"),
        "labeled_rows_10m": window_10m.get("labeled_rows"),
        "queued_rows_10m": window_10m.get("queued_rows"),
        "emitted_rows_10m": window_10m.get("emitted_rows"),
        "verified_rows_10m": window_10m.get("verified_rows"),
        "verification_failed_rows_10m": window_10m.get("verification_failed_rows"),
        "queued_count": current.get("queued_count"),
        "pending_verification_count": current.get("pending_verification_count"),
        "verifying_count": current.get("verifying_count"),
        "published_count": current.get("published_count"),
        "fresh_2_10m_emitted": fresh_2_10m.get("emitted_count"),
        "fresh_2_10m_forced_visible": fresh_2_10m.get("forced_visible_count"),
        "fresh_2_10m_forced_visible_pct": fresh_2_10m.get("forced_visible_pct"),
        "fresh_10_30m_emitted": fresh_10_30m.get("emitted_count"),
        "fresh_10_30m_forced_visible": fresh_10_30m.get("forced_visible_count"),
        "fresh_10_30m_forced_visible_pct": fresh_10_30m.get("forced_visible_pct"),
        "pending_age_0_2m": pending_0_2m.get("total_count"),
        "pending_age_2_10m": pending_2_10m.get("total_count"),
        "pending_age_10_30m": pending_10_30m.get("total_count"),
        "pending_age_30_60m": pending_30_60m.get("total_count"),
        "pending_age_1_4h": pending_1_4h.get("total_count"),
        "pending_age_4h_plus": pending_4h.get("total_count"),
        "label_apply_count": process.get("label_apply_count"),
        "label_verify_count": process.get("label_verify_count"),
        "service_record_cid": service_record.get("cid"),
        "service_record_createdAt": service_record.get("createdAt"),
    }


def detect_service_record_changes(snapshots: list[dict[str, Any]]) -> list[dict[str, Any]]:
    changes: list[dict[str, Any]] = []
    previous_cid: str | None = None

    for snapshot in snapshots:
        service_record = snapshot.get("service_record") or {}
        cid = service_record.get("cid")
        if not cid:
            continue
        if previous_cid is None:
            previous_cid = cid
            continue
        if cid != previous_cid:
            changes.append(
                {
                    "ts": snapshot.get("generated_at_utc"),
                    "new_cid": cid,
                    "createdAt": service_record.get("createdAt"),
                }
            )
            previous_cid = cid

    return changes


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build dashboard-ready JSON payload from collected pipeline JSONL snapshots."
    )
    parser.add_argument(
        "--input-jsonl",
        default="metrics/pipeline_timeseries.jsonl",
        help="Path to collected JSONL snapshots.",
    )
    parser.add_argument(
        "--points",
        type=int,
        default=240,
        help="Number of recent points to include.",
    )
    args = parser.parse_args()

    input_path = Path(args.input_jsonl)
    if not input_path.exists():
        raise SystemExit(f"Input file not found: {input_path}")

    snapshots = load_recent_snapshots(input_path, args.points)
    if not snapshots:
        raise SystemExit(f"No snapshots found in: {input_path}")

    latest = snapshots[-1]
    series = [build_series_point(item) for item in snapshots]
    service_record_changes = detect_service_record_changes(snapshots)

    payload = {
        "generated_at_utc": latest.get("generated_at_utc"),
        "source_file": str(input_path),
        "points_included": len(series),
        "latest_snapshot": latest,
        "series": series,
        "service_record_changes": service_record_changes,
    }

    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
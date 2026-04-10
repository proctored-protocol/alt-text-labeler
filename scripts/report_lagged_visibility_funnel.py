from __future__ import annotations

import glob
import json
import os
from datetime import datetime, timezone
from pathlib import Path


def parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def load_rows(pattern: str) -> list[dict]:
    rows: dict[tuple[str, str], dict] = {}

    for path in sorted(glob.glob(pattern)):
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                row = json.loads(line)
                key = (row["cohort_start_utc"], row["cohort_end_utc"])
                prev = rows.get(key)
                if prev is None or row["generated_at_utc"] > prev["generated_at_utc"]:
                    rows[key] = row

    return sorted(rows.values(), key=lambda r: r["cohort_start_utc"])


def safe_ratio(num: int, den: int) -> float | None:
    if den <= 0:
        return None
    return round(num / den, 4)


def main() -> None:
    input_glob = os.getenv("INPUT_GLOB", "metrics/lagged_visibility_monitor_*.jsonl")
    lookback_minutes = int(os.getenv("LOOKBACK_MINUTES", "60"))

    all_rows = load_rows(input_glob)
    if not all_rows:
        raise SystemExit(f"No rows found for pattern: {input_glob}")

    now = datetime.now(timezone.utc)
    cutoff = now.timestamp() - (lookback_minutes * 60)

    rows = [
        r for r in all_rows
        if parse_dt(r["cohort_end_utc"]).timestamp() >= cutoff
    ]

    if not rows:
        raise SystemExit("No cohort rows in requested lookback window.")

    sums = {
        "head_post_creates": 0,
        "head_image_posts": 0,
        "head_missing_alt_posts": 0,
        "head_partial_alt_posts": 0,
        "head_eligible_posts": 0,
        "intake_rows": 0,
        "applied_rows": 0,
        "skipped_rows": 0,
        "pending_rows": 0,
        "leased_rows": 0,
        "publish_jobs_total": 0,
        "publish_jobs_published": 0,
        "visible": 0,
        "not_visible": 0,
        "not_found": 0,
        "other_error_total": 0,
        "applied_no_publish_job": 0,
        "publish_not_published": 0,
        "published_not_visible": 0,
        "published_not_found": 0,
        "published_other_error": 0,
    }

    for r in rows:
        for k in sums:
            sums[k] += int(r.get(k, 0) or 0)

    visible_denominator = sums["publish_jobs_published"] - sums["not_found"]

    result = {
        "lookback_minutes": lookback_minutes,
        "cohorts_count": len(rows),
        "range": {
            "first_cohort_start_utc": rows[0]["cohort_start_utc"],
            "last_cohort_end_utc": rows[-1]["cohort_end_utc"],
        },
        "absolute_counts": sums,
        "ratios": {
            "image_posts_vs_post_creates": safe_ratio(sums["head_image_posts"], sums["head_post_creates"]),
            "eligible_vs_image_posts": safe_ratio(sums["head_eligible_posts"], sums["head_image_posts"]),
            "intake_vs_head_image": safe_ratio(sums["intake_rows"], sums["head_image_posts"]),
            "applied_vs_head_eligible": safe_ratio(sums["applied_rows"], sums["head_eligible_posts"]),
            "publish_jobs_vs_applied": safe_ratio(sums["publish_jobs_total"], sums["applied_rows"]),
            "published_vs_applied": safe_ratio(sums["publish_jobs_published"], sums["applied_rows"]),
            "visible_vs_published_excluding_not_found": safe_ratio(sums["visible"], visible_denominator),
            "visible_vs_head_eligible": safe_ratio(sums["visible"], sums["head_eligible_posts"]),
            "failed_to_get_visible_label_within_5m_vs_head_eligible": safe_ratio(
                sums["applied_no_publish_job"]
                + sums["publish_not_published"]
                + sums["published_not_visible"]
                + sums["published_other_error"],
                sums["head_eligible_posts"],
            ),
            "not_found_vs_head_eligible": safe_ratio(sums["not_found"], sums["head_eligible_posts"]),
        },
        "decomposition": {
            "pre_publish_gap": max(sums["head_eligible_posts"] - sums["applied_rows"], 0),
            "applied_no_publish_job": sums["applied_no_publish_job"],
            "publish_not_published": sums["publish_not_published"],
            "published_not_visible": sums["published_not_visible"],
            "published_other_error": sums["published_other_error"],
            "published_not_found": sums["published_not_found"],
        },
    }

    print(json.dumps(result, indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
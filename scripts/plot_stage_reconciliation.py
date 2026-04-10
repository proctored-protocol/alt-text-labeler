from __future__ import annotations

import csv
import json
import os
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from datetime import datetime


def parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def main() -> None:
    input_json = Path(os.getenv("INPUT_JSON", "metrics/stage_reconciliation_latest.json"))
    output_png = Path(os.getenv("OUTPUT_PNG", input_json.with_suffix(".png")))
    output_csv = Path(os.getenv("OUTPUT_CSV", input_json.with_suffix(".csv")))

    with input_json.open("r", encoding="utf-8") as f:
        payload = json.load(f)

    rows = payload.get("rows", [])
    if not rows:
        raise SystemExit(f"No rows found in {input_json}")

    minute_bucket = [parse_dt(r["minute_bucket"]) for r in rows]

    head_image_posts = [r["head_image_posts"] for r in rows]
    intake_rows = [r["intake_rows"] for r in rows]

    head_eligible_posts = [r["head_eligible_posts"] for r in rows]
    intake_applied = [r["intake_applied"] for r in rows]

    intake_minus_head_image = [r["intake_minus_head_image"] for r in rows]
    apply_minus_head_eligible = [r["apply_minus_head_eligible"] for r in rows]
    publish_jobs_minus_applied = [r["publish_jobs_minus_applied"] for r in rows]
    published_minus_applied = [r["published_minus_applied"] for r in rows]

    intake_vs_head_image_ratio = [
        None if r["intake_vs_head_image_ratio"] is None else float(r["intake_vs_head_image_ratio"])
        for r in rows
    ]
    apply_vs_head_eligible_ratio = [
        None if r["apply_vs_head_eligible_ratio"] is None else float(r["apply_vs_head_eligible_ratio"])
        for r in rows
    ]
    published_vs_applied_ratio = [
        None if r["published_vs_applied_ratio"] is None else float(r["published_vs_applied_ratio"])
        for r in rows
    ]

    # Write CSV as well, so you can inspect in a sheet if needed
    fieldnames = list(rows[0].keys())
    with output_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    fig, axes = plt.subplots(4, 1, figsize=(16, 14), sharex=True)

    # 1) Intake vs head image posts
    axes[0].plot(minute_bucket, head_image_posts, label="head_image_posts")
    axes[0].plot(minute_bucket, intake_rows, label="intake_rows")
    axes[0].set_title("Head image posts vs Intake rows")
    axes[0].set_ylabel("posts/min")
    axes[0].legend()
    axes[0].grid(True, alpha=0.3)

    # 2) Apply vs head eligible posts
    axes[1].plot(minute_bucket, head_eligible_posts, label="head_eligible_posts")
    axes[1].plot(minute_bucket, intake_applied, label="intake_applied")
    axes[1].set_title("Head eligible posts vs Apply output")
    axes[1].set_ylabel("posts/min")
    axes[1].legend()
    axes[1].grid(True, alpha=0.3)

    # 3) Deltas
    axes[2].plot(minute_bucket, intake_minus_head_image, label="intake_minus_head_image")
    axes[2].plot(minute_bucket, apply_minus_head_eligible, label="apply_minus_head_eligible")
    axes[2].plot(minute_bucket, publish_jobs_minus_applied, label="publish_jobs_minus_applied")
    axes[2].plot(minute_bucket, published_minus_applied, label="published_minus_applied")
    axes[2].axhline(0, linewidth=1)
    axes[2].set_title("Per-minute reconciliation deltas")
    axes[2].set_ylabel("delta")
    axes[2].legend()
    axes[2].grid(True, alpha=0.3)

    # 4) Ratios
    axes[3].plot(minute_bucket, intake_vs_head_image_ratio, label="intake_vs_head_image_ratio")
    axes[3].plot(minute_bucket, apply_vs_head_eligible_ratio, label="apply_vs_head_eligible_ratio")
    axes[3].plot(minute_bucket, published_vs_applied_ratio, label="published_vs_applied_ratio")
    axes[3].axhline(1.0, linewidth=1)
    axes[3].set_title("Per-minute stage ratios")
    axes[3].set_ylabel("ratio")
    axes[3].set_xlabel("UTC minute")
    axes[3].legend()
    axes[3].grid(True, alpha=0.3)

    axes[3].xaxis.set_major_locator(mdates.AutoDateLocator())
    axes[3].xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M"))

    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(output_png, dpi=150, bbox_inches="tight")

    print(json.dumps({
        "input_json": str(input_json),
        "output_png": str(output_png),
        "output_csv": str(output_csv),
        "row_count": len(rows),
    }, indent=2))


if __name__ == "__main__":
    main()
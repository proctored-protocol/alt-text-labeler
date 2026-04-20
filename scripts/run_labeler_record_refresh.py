from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def default_log_path(metrics_dir: Path) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return metrics_dir / f"labeler_record_refresh_{stamp}.jsonl"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run labeler service record refresh on a fixed time interval."
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=int(os.environ.get("REFRESH_INTERVAL_SECONDS", "300")),
        help="Seconds between refresh attempts. Default: 300.",
    )
    parser.add_argument(
        "--reset-script",
        default=os.environ.get(
            "RESET_SCRIPT",
            "scripts/delete_and_recreate_labeler_record.py",
        ),
        help="Path to the reset script.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=int(os.environ.get("RESET_TIMEOUT_SECONDS", "180")),
        help="Subprocess timeout for one reset attempt.",
    )
    parser.add_argument(
        "--metrics-dir",
        default=os.environ.get("METRICS_DIR", "metrics"),
        help="Directory for JSONL logs.",
    )
    parser.add_argument(
        "--startup-reset",
        action="store_true",
        help="Perform one reset immediately on startup.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run exactly one reset attempt and exit.",
    )
    return parser.parse_args()


def run_once(repo_root: Path, reset_script: str, timeout_seconds: int) -> dict:
    started_at = now_iso()
    proc = subprocess.run(
        ["python", reset_script],
        cwd=str(repo_root),
        text=True,
        capture_output=True,
        timeout=timeout_seconds,
    )

    payload = {
        "event": "labeler_record_refresh_attempt",
        "started_at": started_at,
        "finished_at": now_iso(),
        "reset_script": reset_script,
        "returncode": proc.returncode,
        "stdout_tail": proc.stdout[-4000:],
        "stderr_tail": proc.stderr[-4000:],
    }
    payload["success"] = proc.returncode == 0
    return payload


def append_jsonl(path: Path, payload: dict) -> None:
    line = json.dumps(payload, ensure_ascii=False)
    print(line, flush=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def main() -> None:
    args = parse_args()

    repo_root = Path.cwd()
    metrics_dir = Path(args.metrics_dir)
    metrics_dir.mkdir(parents=True, exist_ok=True)
    log_path = default_log_path(metrics_dir)

    startup = {
        "event": "labeler_record_refresh_runner_started",
        "started_at": now_iso(),
        "interval_seconds": args.interval_seconds,
        "reset_script": args.reset_script,
        "timeout_seconds": args.timeout_seconds,
        "startup_reset": bool(args.startup_reset),
        "once": bool(args.once),
        "log_path": str(log_path),
    }
    append_jsonl(log_path, startup)

    if args.once:
        append_jsonl(
            log_path,
            run_once(repo_root, args.reset_script, args.timeout_seconds),
        )
        return

    if args.startup_reset:
        append_jsonl(
            log_path,
            run_once(repo_root, args.reset_script, args.timeout_seconds),
        )

    while True:
        next_run_at = time.time() + args.interval_seconds
        heartbeat = {
            "event": "labeler_record_refresh_waiting",
            "checked_at": now_iso(),
            "next_run_at_utc": datetime.fromtimestamp(next_run_at, tz=timezone.utc).isoformat().replace("+00:00", "Z"),
            "interval_seconds": args.interval_seconds,
        }
        append_jsonl(log_path, heartbeat)

        sleep_for = max(args.interval_seconds, 1)
        time.sleep(sleep_for)

        append_jsonl(
            log_path,
            run_once(repo_root, args.reset_script, args.timeout_seconds),
        )


if __name__ == "__main__":
    main()
from __future__ import annotations

import json
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import text

from app.db import get_engine, session_scope
from app.head.lag import get_consumer_lag_snapshot


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def json_log(payload: dict, log_path: Path) -> None:
    line = json.dumps(payload, ensure_ascii=False, default=str)
    print(line, flush=True)
    with log_path.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def get_head_and_intake():
    from app.db import SessionLocal

    with SessionLocal() as session:
        head = get_consumer_lag_snapshot(session, consumer_name="head_tracker")
        intake = get_consumer_lag_snapshot(session, consumer_name="intake")
    return head, intake


def compute_recovery_start_cursor(start_lag_seconds: int) -> int:
    with get_engine().connect() as conn:
        row = conn.execute(text("""
            SELECT bucket_second, head_seq
            FROM firehose_head_sample
            WHERE bucket_second <= NOW() - (:lag_seconds * INTERVAL '1 second')
            ORDER BY bucket_second DESC
            LIMIT 1
        """), {"lag_seconds": start_lag_seconds}).mappings().one_or_none()

        if row is None:
            row = conn.execute(text("""
                SELECT bucket_second, head_seq
                FROM firehose_head_sample
                ORDER BY bucket_second DESC
                LIMIT 1
            """)).mappings().one()

    return int(row["head_seq"])


def stop_downstream_workers(repo_root: Path) -> None:
    commands = [
        "pkill -f 'scripts/run_intake_worker.py' || true",
        "pkill -f 'scripts/run_apply_worker.py' || true",
        "pkill -f 'scripts/run_publish_worker.py' || true",
        "pkill -f 'scripts/run_visibility_worker.py' || true",
    ]
    for cmd in commands:
        subprocess.run(["bash", "-lc", cmd], cwd=str(repo_root), check=False)
    time.sleep(2)


def wipe_live_pipeline_state() -> None:
    with session_scope() as session:
        session.execute(text("DELETE FROM visibility_check"))
        session.execute(text("DELETE FROM publish_attempt"))
        session.execute(text("DELETE FROM publish_job"))
        session.execute(text("DELETE FROM label_decision"))
        session.execute(text("DELETE FROM manual_override"))
        session.execute(text("DELETE FROM intake_item"))
        session.execute(text("DELETE FROM control_action_log"))

        session.execute(text("""
            DELETE FROM worker_heartbeat
            WHERE stage IN ('apply', 'publish', 'visibility')
        """))

        session.execute(text("""
            DELETE FROM consumer_state
            WHERE consumer_name <> 'head_tracker'
        """))


def run_reset_script(repo_root: Path, reset_script: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["python", reset_script],
        cwd=str(repo_root),
        text=True,
        capture_output=True,
        timeout=180,
        check=False,
    )


def launch_detached(
    *,
    repo_root: Path,
    log_dir: Path,
    name: str,
    argv: list[str],
    extra_env: dict[str, str] | None = None,
) -> int:
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)

    log_file = log_dir / f"{name}.log"
    fh = log_file.open("a", encoding="utf-8")

    proc = subprocess.Popen(
        argv,
        cwd=str(repo_root),
        env=env,
        stdout=fh,
        stderr=subprocess.STDOUT,
        start_new_session=True,
        text=True,
    )
    return proc.pid


def main() -> None:
    repo_root = Path.cwd()
    log_dir = repo_root / "logs" / "live_recovery"
    log_dir.mkdir(parents=True, exist_ok=True)

    metrics_dir = repo_root / "metrics"
    metrics_dir.mkdir(parents=True, exist_ok=True)
    metrics_path = metrics_dir / f"live_recovery_watchdog_{now_utc().strftime('%Y%m%dT%H%M%SZ')}.jsonl"

    poll_seconds = int(os.getenv("WATCHDOG_POLL_SECONDS", "60"))
    consecutive_breaches_required = int(os.getenv("WATCHDOG_CONSECUTIVE_BREACHES", "5"))
    intake_lag_threshold_seconds = int(os.getenv("INTAKE_LAG_THRESHOLD_SECONDS", "600"))
    intake_seq_gap_threshold = int(os.getenv("INTAKE_SEQ_GAP_THRESHOLD", "500000"))
    recovery_start_lag_seconds = int(os.getenv("RECOVERY_INTAKE_START_LAG_SECONDS", "60"))
    recovery_publish_workers = int(os.getenv("RECOVERY_PUBLISH_WORKERS", "1"))
    recovery_publish_batch_size = int(os.getenv("RECOVERY_PUBLISH_BATCH_SIZE", "50"))
    recovery_reset_script = os.getenv(
        "RECOVERY_RESET_SCRIPT",
        "archive/legacy-v1/scripts/delete_and_recreate_labeler_record.py",
    )
    enable_visibility = env_bool("RECOVERY_ENABLE_VISIBILITY", True)

    breach_count = 0
    recovery_count = 0

    while True:
        started = time.monotonic()
        payload: dict[str, object] = {
            "generated_at_utc": now_utc().isoformat(),
            "poll_seconds": poll_seconds,
            "consecutive_breaches_required": consecutive_breaches_required,
            "intake_lag_threshold_seconds": intake_lag_threshold_seconds,
            "intake_seq_gap_threshold": intake_seq_gap_threshold,
            "recovery_start_lag_seconds": recovery_start_lag_seconds,
            "recovery_publish_workers": recovery_publish_workers,
            "recovery_publish_batch_size": recovery_publish_batch_size,
        }

        try:
            head, intake = get_head_and_intake()

            lag_seconds = intake.lag_seconds_estimate
            seq_gap = intake.seq_gap_to_head
            lag_breach = lag_seconds is not None and lag_seconds > intake_lag_threshold_seconds
            seq_breach = seq_gap is not None and seq_gap > intake_seq_gap_threshold
            breach = bool(lag_breach or seq_breach)

            if breach:
                breach_count += 1
            else:
                breach_count = 0

            payload["head_tracker"] = head.to_dict()
            payload["intake"] = intake.to_dict()
            payload["breach"] = {
                "lag_breach": lag_breach,
                "seq_breach": seq_breach,
                "breach_count": breach_count,
                "should_recover": breach_count >= consecutive_breaches_required,
            }

            if breach_count >= consecutive_breaches_required:
                recovery_count += 1

                stop_downstream_workers(repo_root)
                wipe_live_pipeline_state()
                start_cursor = compute_recovery_start_cursor(recovery_start_lag_seconds)
                reset_proc = run_reset_script(repo_root, recovery_reset_script)

                launched = []
                launched.append({
                    "stage": "intake",
                    "pid": launch_detached(
                        repo_root=repo_root,
                        log_dir=log_dir,
                        name=f"intake_recovery_{recovery_count}",
                        argv=["python", "scripts/run_intake_worker.py"],
                        extra_env={
                            "INTAKE_RESUME_FROM_CONSUMER_STATE": "false",
                            "INTAKE_START_CURSOR": str(start_cursor),
                            "INTAKE_LIVE_MODE": "true",
                        },
                    ),
                })

                time.sleep(3)

                launched.append({
                    "stage": "apply",
                    "pid": launch_detached(
                        repo_root=repo_root,
                        log_dir=log_dir,
                        name=f"apply_recovery_{recovery_count}",
                        argv=["python", "scripts/run_apply_worker.py"],
                    ),
                })

                time.sleep(2)

                for idx in range(recovery_publish_workers):
                    launched.append({
                        "stage": "publish",
                        "pid": launch_detached(
                            repo_root=repo_root,
                            log_dir=log_dir,
                            name=f"publish_recovery_{recovery_count}_{idx+1}",
                            argv=["python", "scripts/run_publish_worker.py"],
                            extra_env={
                                "PUBLISH_BATCH_SIZE": str(recovery_publish_batch_size),
                                "PUBLISH_MAX_ATTEMPTS": "3",
                                "PUBLISH_BACKOFF_BASE_SECONDS": "60",
                            },
                        ),
                    })

                if enable_visibility:
                    time.sleep(2)
                    launched.append({
                        "stage": "visibility",
                        "pid": launch_detached(
                            repo_root=repo_root,
                            log_dir=log_dir,
                            name=f"visibility_recovery_{recovery_count}",
                            argv=["python", "scripts/run_visibility_worker.py"],
                            extra_env={
                                "VISIBILITY_MAX_AGE_SECONDS": os.getenv("VISIBILITY_MAX_AGE_SECONDS", "1800"),
                            },
                        ),
                    })

                payload["recovery_action"] = {
                    "performed": True,
                    "recovery_count": recovery_count,
                    "new_intake_start_cursor": start_cursor,
                    "reset_script": recovery_reset_script,
                    "reset_returncode": reset_proc.returncode,
                    "reset_stdout_tail": reset_proc.stdout[-4000:],
                    "reset_stderr_tail": reset_proc.stderr[-4000:],
                    "launched": launched,
                }

                breach_count = 0
            else:
                payload["recovery_action"] = {"performed": False}

        except Exception as exc:
            payload["loop_error"] = {
                "type": exc.__class__.__name__,
                "text": str(exc),
            }

        json_log(payload, metrics_path)

        elapsed = time.monotonic() - started
        time.sleep(max(0.0, poll_seconds - elapsed))


if __name__ == "__main__":
    main()
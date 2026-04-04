from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timezone
from typing import Any

from atproto import (
    FirehoseSubscribeReposClient,
    models,
    parse_subscribe_repos_message,
)
from sqlalchemy import text

from app.config import get_settings
from app.db import engine


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat()


class FirehoseHeadProbe:
    def __init__(
        self,
        *,
        base_uri: str,
        sample_size: int = 5,
        timeout_seconds: float = 5.0,
    ) -> None:
        self.base_uri = base_uri
        self.sample_size = sample_size
        self.timeout_seconds = timeout_seconds

        self.samples: list[dict[str, Any]] = []
        self.errors: list[str] = []

        self._lock = threading.Lock()
        self._done = threading.Event()
        self._client = FirehoseSubscribeReposClient(
            params=None,
            base_uri=base_uri,
            recv_timeout=30.0,
        )
        self._thread: threading.Thread | None = None

    def _maybe_record_seq(self, parsed: Any) -> None:
        seq = getattr(parsed, "seq", None)
        if seq is None:
            return

        with self._lock:
            self.samples.append(
                {
                    "seq": int(seq),
                    "py_type": type(parsed).__name__,
                    "captured_at": iso_now(),
                }
            )
            if len(self.samples) >= self.sample_size:
                self._done.set()

    def _on_message(self, message) -> None:
        parsed = parse_subscribe_repos_message(message)

        if isinstance(
            parsed,
            (
                models.ComAtprotoSyncSubscribeRepos.Commit,
                models.ComAtprotoSyncSubscribeRepos.Identity,
                models.ComAtprotoSyncSubscribeRepos.Account,
                models.ComAtprotoSyncSubscribeRepos.Sync,
            ),
        ):
            self._maybe_record_seq(parsed)
            return

        # Fallback for any other frame type that still carries seq.
        self._maybe_record_seq(parsed)

    def _on_callback_error(self, exc: BaseException) -> None:
        with self._lock:
            self.errors.append(str(exc))
        self._done.set()

    def _run_client(self) -> None:
        try:
            self._client.start(self._on_message, self._on_callback_error)
        except Exception as exc:
            with self._lock:
                self.errors.append(str(exc))
            self._done.set()

    def probe(self) -> dict[str, Any]:
        started_at = time.monotonic()

        self._thread = threading.Thread(target=self._run_client, daemon=True)
        self._thread.start()

        self._done.wait(self.timeout_seconds)

        try:
            self._client.stop()
        except Exception:
            pass

        if self._thread is not None:
            self._thread.join(timeout=5.0)

        elapsed = round(time.monotonic() - started_at, 3)

        with self._lock:
            samples = list(self.samples)
            errors = list(self.errors)

        seqs = [item["seq"] for item in samples]
        head_seq = max(seqs) if seqs else None

        return {
            "probe_base_uri": self.base_uri,
            "sample_size_requested": self.sample_size,
            "sample_size_observed": len(samples),
            "elapsed_seconds": elapsed,
            "head_seq": head_seq,
            "samples": samples,
            "errors": errors,
        }


def main() -> None:
    settings = get_settings()

    with engine.connect() as conn:
        cursor_row = conn.execute(
            text(
                """
                SELECT stream_name, last_seq, updated_at
                FROM firehose_cursor
                WHERE stream_name = 'subscribe_repos'
                """
            )
        ).mappings().first()

        evaluation_row = conn.execute(
            text(
                """
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
                """
            )
        ).mappings().first()

    probe = FirehoseHeadProbe(
        base_uri=settings.firehose_base_uri,
        sample_size=5,
        timeout_seconds=5.0,
    ).probe()

    saved_cursor = cursor_row["last_seq"] if cursor_row else None
    saved_cursor_updated_at = cursor_row["updated_at"] if cursor_row else None

    max_last_seen_seq = evaluation_row["max_last_seen_seq"] if evaluation_row else None
    max_evaluated_at = evaluation_row["max_evaluated_at"] if evaluation_row else None
    recent_post_evaluation_rows = (
        evaluation_row["recent_post_evaluation_rows"] if evaluation_row else None
    )
    recent_labeled_rows = evaluation_row["recent_labeled_rows"] if evaluation_row else None

    head_seq = probe["head_seq"]

    gap_vs_saved_cursor = None
    if head_seq is not None and saved_cursor is not None:
        gap_vs_saved_cursor = int(head_seq) - int(saved_cursor)

    gap_vs_max_last_seen_seq = None
    if head_seq is not None and max_last_seen_seq is not None:
        gap_vs_max_last_seen_seq = int(head_seq) - int(max_last_seen_seq)

    payload = {
        "generated_at_utc": iso_now(),
        "firehose_base_uri": settings.firehose_base_uri,
        "saved_cursor": {
            "stream_name": "subscribe_repos",
            "last_seq": saved_cursor,
            "updated_at": saved_cursor_updated_at,
        },
        "post_evaluation_state": {
            "max_last_seen_seq": max_last_seen_seq,
            "max_evaluated_at": max_evaluated_at,
            "recent_post_evaluation_rows": recent_post_evaluation_rows,
            "recent_labeled_rows": recent_labeled_rows,
        },
        "head_probe": probe,
        "cursor_gap": {
            "gap_vs_saved_cursor": gap_vs_saved_cursor,
            "gap_vs_max_last_seen_seq": gap_vs_max_last_seen_seq,
        },
    }

    print(json.dumps(payload, indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
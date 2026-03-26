from __future__ import annotations

import argparse
import json
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text

from app.db import SessionLocal, engine, init_db
from app.services.label_work_queue import enqueue_label_work_item


PUBLIC_API_BASE_URL = "https://public.api.bsky.app"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat()


def print_json_line(data: dict[str, Any]) -> None:
    print(json.dumps(data, ensure_ascii=False, default=str), flush=True)


def resolve_did_to_handle(did: str) -> str:
    url = (
        f"{PUBLIC_API_BASE_URL}/xrpc/app.bsky.actor.getProfile?"
        + urllib.parse.urlencode({"actor": did})
    )
    req = urllib.request.Request(url, headers={"Accept": "application/json"}, method="GET")
    with urllib.request.urlopen(req, timeout=30) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    handle = payload.get("handle")
    if not handle:
        raise RuntimeError(f"Could not resolve DID to handle: {did}")
    return handle


def uri_to_post_url(uri: str) -> str:
    prefix = "at://"
    if not uri.startswith(prefix):
        raise ValueError(f"Unexpected AT URI: {uri}")

    rest = uri[len(prefix):]
    parts = rest.split("/")
    if len(parts) != 3:
        raise ValueError(f"Unexpected AT URI shape: {uri}")

    did, collection, rkey = parts
    if collection != "app.bsky.feed.post":
        raise ValueError(f"Unexpected collection in URI: {uri}")

    handle = resolve_did_to_handle(did)
    return f"https://bsky.app/profile/{handle}/post/{rkey}"


def fetch_unqueued_candidates(*, batch_size: int, lookback_hours: int) -> list[dict[str, Any]]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT
                    pe.uri,
                    pe.cid,
                    pe.derived_label,
                    pe.record_created_at,
                    pe.evaluated_at
                FROM post_evaluation pe
                LEFT JOIN label_work_item lwi
                  ON lwi.uri = pe.uri
                 AND lwi.cid = pe.cid
                 AND lwi.label_value = pe.derived_label
                WHERE pe.derived_label IN ('missing-alt-text', 'partial-alt-text')
                  AND pe.evaluated_at >= NOW() - (:lookback_hours * INTERVAL '1 hour')
                  AND lwi.id IS NULL
                ORDER BY pe.evaluated_at ASC
                LIMIT :batch_size
                """
            ),
            {
                "batch_size": batch_size,
                "lookback_hours": lookback_hours,
            },
        ).mappings().all()
    return [dict(row) for row in rows]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Continuously enqueue newly evaluated labeled posts into label_work_item."
    )
    parser.add_argument("--poll-seconds", type=float, default=2.0)
    parser.add_argument("--batch-size", type=int, default=250)
    parser.add_argument("--lookback-hours", type=int, default=24)
    args = parser.parse_args()

    init_db()

    print_json_line(
        {
            "event": "candidate_enqueue_worker_started",
            "checked_at": iso_now(),
            "poll_seconds": args.poll_seconds,
            "batch_size": args.batch_size,
            "lookback_hours": args.lookback_hours,
        }
    )

    while True:
        rows = fetch_unqueued_candidates(
            batch_size=args.batch_size,
            lookback_hours=args.lookback_hours,
        )

        if not rows:
            time.sleep(args.poll_seconds)
            continue

        enqueued = 0
        failed = 0

        with SessionLocal() as session:
            for row in rows:
                try:
                    post_url = uri_to_post_url(row["uri"])
                    enqueue_label_work_item(
                        session,
                        uri=row["uri"],
                        cid=row["cid"],
                        label_value=row["derived_label"],
                        post_url=post_url,
                        record_created_at=str(row["record_created_at"]) if row["record_created_at"] is not None else None,
                        evaluated_at=str(row["evaluated_at"]) if row["evaluated_at"] is not None else None,
                    )
                    enqueued += 1
                except Exception as exc:
                    failed += 1
                    print_json_line(
                        {
                            "event": "candidate_enqueue_failed",
                            "checked_at": iso_now(),
                            "post_uri": row.get("uri"),
                            "post_cid": row.get("cid"),
                            "label_value": row.get("derived_label"),
                            "error": str(exc),
                        }
                    )
            session.commit()

        print_json_line(
            {
                "event": "candidate_enqueue_batch_complete",
                "checked_at": iso_now(),
                "fetched": len(rows),
                "enqueued": enqueued,
                "failed": failed,
            }
        )


if __name__ == "__main__":
    main()
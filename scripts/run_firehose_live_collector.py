from __future__ import annotations

import argparse
import logging
import sys

from app.firehose.live_monitor import FirehoseLiveCollector


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Collect live firehose metrics into a standalone SQLite store."
    )
    parser.add_argument(
        "--db-path",
        default="data/firehose_live.sqlite3",
        help="SQLite database file for live firehose metrics",
    )
    parser.add_argument(
        "--base-uri",
        default="wss://bsky.network/xrpc",
        help="Firehose base URI",
    )
    parser.add_argument(
        "--cursor",
        type=int,
        default=None,
        help="Optional explicit firehose cursor to start from",
    )
    parser.add_argument(
        "--resume-from-store",
        action="store_true",
        help="Resume from the last seq stored in the SQLite state table",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Python logging level",
    )
    args = parser.parse_args()

    configure_logging(args.log_level)

    worker = FirehoseLiveCollector(
        db_path=args.db_path,
        base_uri=args.base_uri,
        cursor=args.cursor,
        resume_from_store=args.resume_from_store,
    )
    worker.run()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("stopped", file=sys.stderr)
        raise SystemExit(130)
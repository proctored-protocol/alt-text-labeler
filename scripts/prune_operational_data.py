#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from sqlalchemy import create_engine, text


TABLES = [
    "label_visibility",
    "label_publication",
    "publish_job",
    "post_evaluation",
]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--older-than-days", type=int, default=3)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    database_url = os.environ["DATABASE_URL"]
    engine = create_engine(database_url, pool_pre_ping=True, future=True)

    ts_expr = "now() - (:days || ' days')::interval"

    with engine.begin() as conn:
        for table in TABLES:
            count_sql = text(f"select count(*) from {table} where coalesce(updated_at, created_at, evaluated_at, published_at) < {ts_expr}")
            delete_sql = text(f"delete from {table} where coalesce(updated_at, created_at, evaluated_at, published_at) < {ts_expr}")

            count_ = conn.execute(count_sql, {"days": args.older_than_days}).scalar_one()
            if args.dry_run:
                print({"table": table, "would_delete": count_})
            else:
                conn.execute(delete_sql, {"days": args.older_than_days})
                print({"table": table, "deleted": count_})


if __name__ == "__main__":
    main()
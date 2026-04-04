from __future__ import annotations

import json

from sqlalchemy import text

from app.db import engine


def main() -> None:
    with engine.connect() as conn:
        by_state = conn.execute(
            text(
                """
                SELECT state, COUNT(*) AS n
                FROM label_work_item
                GROUP BY state
                ORDER BY state
                """
            )
        ).mappings().all()

        recent = conn.execute(
            text(
                """
                SELECT
                    id,
                    post_url,
                    record_created_at,
                    label_value,
                    state,
                    ozone_created_at,
                    final_forced_found_label,
                    final_query_found_label,
                    manual_success,
                    leased_by,
                    lease_expires_at,
                    last_error,
                    updated_at
                FROM label_work_item
                ORDER BY updated_at DESC
                LIMIT 25
                """
            )
        ).mappings().all()

    print("=== label_work_item counts ===")
    for row in by_state:
        print(dict(row))

    print()
    print("=== recent label_work_item rows ===")
    for row in recent:
        print(json.dumps(dict(row), ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
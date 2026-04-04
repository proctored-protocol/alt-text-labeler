from sqlalchemy import text

from app.db import engine

WINDOWS = [
    ("1 minute", "1m"),
    ("10 minutes", "10m"),
    ("1 hour", "1h"),
    ("24 hours", "24h"),
]


def main() -> None:
    with engine.connect() as conn:
        print("=== queue state counts ===")
        rows = conn.execute(text("""
            SELECT state, COUNT(*) AS n
            FROM publish_job
            GROUP BY state
            ORDER BY state
        """)).mappings().all()
        for row in rows:
            print(dict(row))

        print("\n=== queue age summary ===")
        row = conn.execute(text("""
            SELECT
                COUNT(*) FILTER (WHERE state = 'pending') AS pending_jobs,
                COUNT(*) FILTER (WHERE state = 'leased') AS leased_jobs,
                COUNT(*) FILTER (WHERE state = 'published') AS published_jobs,
                COUNT(*) FILTER (WHERE state = 'dead') AS dead_jobs,
                MIN(created_at) FILTER (WHERE state = 'pending') AS oldest_pending_created_at,
                MIN(created_at) FILTER (WHERE state = 'leased') AS oldest_leased_created_at,
                MIN(next_attempt_at) FILTER (WHERE state = 'pending') AS next_pending_attempt_at
            FROM publish_job
        """)).mappings().one()
        print(dict(row))

        print("\n=== pending/leased age in seconds ===")
        row = conn.execute(text("""
            WITH s AS (
                SELECT
                    MIN(created_at) FILTER (WHERE state = 'pending') AS oldest_pending_created_at,
                    MIN(created_at) FILTER (WHERE state = 'leased') AS oldest_leased_created_at,
                    MIN(updated_at) FILTER (WHERE state = 'leased') AS oldest_leased_updated_at
                FROM publish_job
            )
            SELECT
                ROUND(EXTRACT(EPOCH FROM (NOW() - oldest_pending_created_at))) AS oldest_pending_age_s,
                ROUND(EXTRACT(EPOCH FROM (NOW() - oldest_leased_created_at))) AS oldest_leased_age_s,
                ROUND(EXTRACT(EPOCH FROM (NOW() - oldest_leased_updated_at))) AS oldest_lease_update_age_s
            FROM s
        """)).mappings().one()
        print(dict(row))

        print("\n=== jobs created by recent window ===")
        for interval, tag in WINDOWS:
            row = conn.execute(text(f"""
                SELECT
                    COUNT(*) AS created_jobs,
                    COUNT(*) FILTER (WHERE state = 'published') AS currently_published_rows,
                    COUNT(*) FILTER (WHERE state = 'pending') AS currently_pending_rows,
                    COUNT(*) FILTER (WHERE state = 'leased') AS currently_leased_rows,
                    COUNT(*) FILTER (WHERE state = 'dead') AS currently_dead_rows
                FROM publish_job
                WHERE created_at >= NOW() - INTERVAL '{interval}'
            """)).mappings().one()
            print(f"{tag}: {dict(row)}")

        print("\n=== jobs updated by recent window ===")
        for interval, tag in WINDOWS:
            row = conn.execute(text(f"""
                SELECT
                    COUNT(*) AS touched_jobs,
                    COUNT(*) FILTER (WHERE state = 'published') AS touched_and_now_published,
                    COUNT(*) FILTER (WHERE state = 'pending') AS touched_and_now_pending,
                    COUNT(*) FILTER (WHERE state = 'leased') AS touched_and_now_leased,
                    COUNT(*) FILTER (WHERE state = 'dead') AS touched_and_now_dead
                FROM publish_job
                WHERE updated_at >= NOW() - INTERVAL '{interval}'
            """)).mappings().one()
            print(f"{tag}: {dict(row)}")

        print("\n=== attempt count histogram ===")
        rows = conn.execute(text("""
            SELECT attempt_count, COUNT(*) AS n
            FROM publish_job
            GROUP BY attempt_count
            ORDER BY attempt_count
            LIMIT 20
        """)).mappings().all()
        for row in rows:
            print(dict(row))

        print("\n=== oldest pending jobs ===")
        rows = conn.execute(text("""
            SELECT
                id, uri, cid, label_value, state, attempt_count,
                created_at, updated_at, next_attempt_at, leased_by, leased_until, last_error
            FROM publish_job
            WHERE state = 'pending'
            ORDER BY created_at ASC
            LIMIT 20
        """)).mappings().all()
        for row in rows:
            print(dict(row))

        print("\n=== current leased jobs ===")
        rows = conn.execute(text("""
            SELECT
                id, uri, cid, label_value, state, attempt_count,
                created_at, updated_at, next_attempt_at, leased_by, leased_until, last_error
            FROM publish_job
            WHERE state = 'leased'
            ORDER BY leased_until ASC NULLS LAST, updated_at ASC
            LIMIT 20
        """)).mappings().all()
        for row in rows:
            print(dict(row))

        print("\n=== latest dead jobs ===")
        rows = conn.execute(text("""
            SELECT
                id, uri, cid, label_value, state, attempt_count,
                created_at, updated_at, next_attempt_at, leased_by, leased_until, last_error
            FROM publish_job
            WHERE state = 'dead'
            ORDER BY updated_at DESC
            LIMIT 20
        """)).mappings().all()
        for row in rows:
            print(dict(row))


if __name__ == "__main__":
    main()
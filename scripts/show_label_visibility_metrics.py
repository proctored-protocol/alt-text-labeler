from sqlalchemy import text

from app.db import engine

WINDOWS = [
    ("1 minute", "1m"),
    ("10 minutes", "10m"),
    ("1 hour", "1h"),
    ("24 hours", "24h"),
]


def show_lag_block(conn, interval: str, tag: str) -> None:
    row = conn.execute(
        text(f"""
            SELECT
                COUNT(*) FILTER (WHERE forced_visible_at IS NOT NULL) AS forced_visible_n,
                COUNT(*) FILTER (WHERE subscriber_visible_at IS NOT NULL) AS subscriber_visible_n,

                ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (forced_visible_at - record_created_at))
                )::numeric, 2) AS created_to_forced_p50_s,
                ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (forced_visible_at - record_created_at))
                )::numeric, 2) AS created_to_forced_p95_s,

                ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (forced_visible_at - first_published_at))
                )::numeric, 2) AS published_to_forced_p50_s,
                ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (forced_visible_at - first_published_at))
                )::numeric, 2) AS published_to_forced_p95_s,

                ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (subscriber_visible_at - record_created_at))
                )::numeric, 2) AS created_to_subscriber_p50_s,
                ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (subscriber_visible_at - record_created_at))
                )::numeric, 2) AS created_to_subscriber_p95_s,

                ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (subscriber_visible_at - first_published_at))
                )::numeric, 2) AS published_to_subscriber_p50_s,
                ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (subscriber_visible_at - first_published_at))
                )::numeric, 2) AS published_to_subscriber_p95_s

            FROM label_visibility
            WHERE first_published_at >= NOW() - INTERVAL '{interval}'
        """)
    ).mappings().one()

    print(f"=== visibility window={tag} ===")
    print(dict(row))
    print()


def main() -> None:
    with engine.connect() as conn:
        print("=== latest visibility activity ===")
        row = conn.execute(
            text("""
                SELECT
                    MAX(first_published_at) AS last_published_at,
                    MAX(forced_visible_at) AS last_forced_visible_at,
                    MAX(subscriber_visible_at) AS last_subscriber_visible_at,
                    COUNT(*) AS tracked_total,
                    COUNT(*) FILTER (WHERE forced_visible_at IS NOT NULL) AS forced_visible_total,
                    COUNT(*) FILTER (WHERE subscriber_visible_at IS NOT NULL) AS subscriber_visible_total,
                    COUNT(*) FILTER (WHERE forced_visible_at IS NULL) AS forced_not_yet_visible_total,
                    COUNT(*) FILTER (WHERE subscriber_visible_at IS NULL) AS subscriber_not_yet_visible_total
                FROM label_visibility
            """)
        ).mappings().one()
        print(dict(row))
        print()

        print("=== recent errors ===")
        rows = conn.execute(
            text("""
                SELECT
                    uri,
                    cid,
                    label_value,
                    last_forced_error,
                    last_subscriber_error,
                    last_forced_checked_at,
                    last_subscriber_checked_at
                FROM label_visibility
                WHERE last_forced_error IS NOT NULL
                   OR last_subscriber_error IS NOT NULL
                ORDER BY updated_at DESC
                LIMIT 20
            """)
        ).mappings().all()
        for row in rows:
            print(dict(row))
        print()

        for interval, tag in WINDOWS:
            show_lag_block(conn, interval, tag)


if __name__ == "__main__":
    main()
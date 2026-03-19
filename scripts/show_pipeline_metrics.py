from sqlalchemy import create_engine, text
from app.config import get_settings

WINDOWS = [
    ("1m", "1 minute"),
    ("10m", "10 minutes"),
    ("1h", "1 hour"),
    ("24h", "24 hours"),
]

LABEL_MISSING = "missing-alt-text"
LABEL_PARTIAL = "partial-alt-text"

engine = create_engine(get_settings().database_url)


def scalar(conn, sql: str, params: dict | None = None):
    return conn.execute(text(sql), params or {}).scalar()


def row_dict(conn, sql: str, params: dict | None = None):
    return conn.execute(text(sql), params or {}).mappings().one()


def print_window_metrics(conn, tag: str, interval: str):
    print(f"\n=== window={tag} ({interval}) ===")

    evaluated = scalar(conn, """
        SELECT COUNT(*)
        FROM post_evaluation
        WHERE evaluated_at >= NOW() - CAST(:interval AS interval)
    """, {"interval": interval})

    derived_missing = scalar(conn, """
        SELECT COUNT(*)
        FROM post_evaluation
        WHERE evaluated_at >= NOW() - CAST(:interval AS interval)
          AND derived_label = :label
    """, {"interval": interval, "label": LABEL_MISSING})

    derived_partial = scalar(conn, """
        SELECT COUNT(*)
        FROM post_evaluation
        WHERE evaluated_at >= NOW() - CAST(:interval AS interval)
          AND derived_label = :label
    """, {"interval": interval, "label": LABEL_PARTIAL})

    published_missing = scalar(conn, """
        SELECT COUNT(*)
        FROM label_publication
        WHERE status = 'published'
          AND published_at IS NOT NULL
          AND published_at >= NOW() - CAST(:interval AS interval)
          AND label_value = :label
    """, {"interval": interval, "label": LABEL_MISSING})

    published_partial = scalar(conn, """
        SELECT COUNT(*)
        FROM label_publication
        WHERE status = 'published'
          AND published_at IS NOT NULL
          AND published_at >= NOW() - CAST(:interval AS interval)
          AND label_value = :label
    """, {"interval": interval, "label": LABEL_PARTIAL})

    print(f"evaluated_image_posts: {evaluated}")
    print(f"derived_missing_alt:   {derived_missing}")
    print(f"derived_partial_alt:   {derived_partial}")
    print(f"published_missing_alt: {published_missing}")
    print(f"published_partial_alt: {published_partial}")

    lag = row_dict(conn, """
        WITH joined AS (
          SELECT
            lp.published_at,
            pe.evaluated_at,
            (pe.record_created_at::timestamptz) AS created_ts
          FROM label_publication lp
          JOIN post_evaluation pe
            ON pe.uri = lp.uri
           AND pe.cid = lp.cid
           AND pe.derived_label = lp.label_value
          WHERE lp.status = 'published'
            AND lp.published_at IS NOT NULL
            AND lp.published_at >= NOW() - CAST(:interval AS interval)
            AND pe.record_created_at IS NOT NULL
        )
        SELECT
          COUNT(*) AS n,
          ROUND(AVG(EXTRACT(EPOCH FROM (evaluated_at - created_ts)))::numeric, 2) AS eval_avg_s,
          ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (
            ORDER BY EXTRACT(EPOCH FROM (evaluated_at - created_ts))
          )::numeric, 2) AS eval_p50_s,
          ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (
            ORDER BY EXTRACT(EPOCH FROM (evaluated_at - created_ts))
          )::numeric, 2) AS eval_p95_s,
          ROUND(AVG(EXTRACT(EPOCH FROM (published_at - created_ts)))::numeric, 2) AS pub_avg_s,
          ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (
            ORDER BY EXTRACT(EPOCH FROM (published_at - created_ts))
          )::numeric, 2) AS pub_p50_s,
          ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (
            ORDER BY EXTRACT(EPOCH FROM (published_at - created_ts))
          )::numeric, 2) AS pub_p95_s
        FROM joined
    """, {"interval": interval})

    print("published_lag_sample_n:", lag["n"])
    print("created_to_evaluated_avg_s:", lag["eval_avg_s"])
    print("created_to_evaluated_p50_s:", lag["eval_p50_s"])
    print("created_to_evaluated_p95_s:", lag["eval_p95_s"])
    print("created_to_published_avg_s:", lag["pub_avg_s"])
    print("created_to_published_p50_s:", lag["pub_p50_s"])
    print("created_to_published_p95_s:", lag["pub_p95_s"])


def main():
    with engine.connect() as conn:
        latest = row_dict(conn, """
            SELECT
              (SELECT MAX(evaluated_at) FROM post_evaluation) AS last_evaluated_at,
              (SELECT MAX(published_at) FROM label_publication WHERE status = 'published') AS last_published_at,
              (SELECT COUNT(*) FROM label_publication WHERE status = 'pending') AS pending_total,
              (SELECT COUNT(*) FROM label_publication WHERE status = 'failed') AS failed_total,
              (SELECT COUNT(*) FROM label_publication WHERE status = 'published') AS published_total
        """)

        print("=== latest activity ===")
        print(f"last_evaluated_at: {latest['last_evaluated_at']}")
        print(f"last_published_at: {latest['last_published_at']}")
        print(f"pending_total:     {latest['pending_total']}")
        print(f"failed_total:      {latest['failed_total']}")
        print(f"published_total:   {latest['published_total']}")

        for tag, interval in WINDOWS:
            print_window_metrics(conn, tag, interval)


if __name__ == "__main__":
    main()
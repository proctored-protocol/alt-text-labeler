from sqlalchemy import create_engine, text
from app.config import get_settings

WINDOWS = [
    ("1m", "1 minute"),
    ("10m", "10 minutes"),
    ("1h", "1 hour"),
    ("24h", "24 hours"),
]

engine = create_engine(get_settings().database_url)

with engine.connect() as conn:
    for tag, interval in WINDOWS:
        row = conn.execute(text("""
            SELECT
              COALESCE(SUM(commit_count), 0) AS commit_count,
              COALESCE(SUM(post_create_count), 0) AS post_create_count,
              COALESCE(SUM(image_post_count), 0) AS image_post_count,
              COALESCE(SUM(image_eval_count), 0) AS image_eval_count,
              COALESCE(SUM(missing_label_count), 0) AS missing_label_count,
              COALESCE(SUM(partial_label_count), 0) AS partial_label_count,
              COALESCE(SUM(publish_success_count), 0) AS publish_success_count,
              COALESCE(SUM(publish_failed_count), 0) AS publish_failed_count
            FROM firehose_minute_stats
            WHERE bucket >= NOW() - CAST(:interval AS interval)
        """), {"interval": interval}).mappings().one()

        print(f"=== firehose window={tag} ({interval}) ===")
        print(dict(row))
        print()
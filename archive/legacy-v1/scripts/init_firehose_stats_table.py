from sqlalchemy import create_engine, text
from app.config import get_settings

engine = create_engine(get_settings().database_url)

sql = """
CREATE TABLE IF NOT EXISTS firehose_minute_stats (
    bucket timestamptz PRIMARY KEY,
    commit_count bigint NOT NULL DEFAULT 0,
    post_create_count bigint NOT NULL DEFAULT 0,
    image_post_count bigint NOT NULL DEFAULT 0,
    image_eval_count bigint NOT NULL DEFAULT 0,
    missing_label_count bigint NOT NULL DEFAULT 0,
    partial_label_count bigint NOT NULL DEFAULT 0,
    publish_success_count bigint NOT NULL DEFAULT 0,
    publish_failed_count bigint NOT NULL DEFAULT 0
)
"""

with engine.begin() as conn:
    conn.execute(text(sql))

print("firehose_minute_stats ready")
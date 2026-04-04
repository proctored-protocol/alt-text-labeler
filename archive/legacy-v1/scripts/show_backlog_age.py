from sqlalchemy import create_engine, text
from app.config import get_settings

engine = create_engine(get_settings().database_url)

sql = text("""
WITH rows AS (
  SELECT
    evaluated_at,
    (record_created_at::timestamptz) AS created_ts
  FROM post_evaluation
  WHERE record_created_at IS NOT NULL
    AND evaluated_at >= NOW() - CAST(:interval AS interval)
)
SELECT
  COUNT(*) AS n,
  ROUND(AVG(EXTRACT(EPOCH FROM (evaluated_at - created_ts)))::numeric, 2) AS avg_age_s,
  ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (
    ORDER BY EXTRACT(EPOCH FROM (evaluated_at - created_ts))
  )::numeric, 2) AS p50_age_s,
  ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (
    ORDER BY EXTRACT(EPOCH FROM (evaluated_at - created_ts))
  )::numeric, 2) AS p95_age_s
FROM rows
""")

intervals = ["10 minutes", "1 hour", "24 hours"]

with engine.connect() as conn:
    for interval in intervals:
        row = conn.execute(sql, {"interval": interval}).mappings().one()
        print(f"=== backlog_age window={interval} ===")
        print(dict(row))
        print()
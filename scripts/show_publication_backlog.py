from sqlalchemy import create_engine, text
from app.config import get_settings

engine = create_engine(get_settings().database_url)

with engine.connect() as conn:
    totals = conn.execute(text("""
        SELECT status, COUNT(*) AS n
        FROM label_publication
        GROUP BY status
        ORDER BY status
    """)).mappings().all()

    print("=== publication totals ===")
    for row in totals:
        print(dict(row))

    print("\n=== top failure messages ===")
    rows = conn.execute(text("""
        SELECT COALESCE(error_text, '<null>') AS error_text, COUNT(*) AS n
        FROM label_publication
        WHERE status = 'failed'
        GROUP BY COALESCE(error_text, '<null>')
        ORDER BY n DESC
        LIMIT 20
    """)).mappings().all()
    for row in rows:
        print(dict(row))

    print("\n=== latest failed rows ===")
    rows = conn.execute(text("""
        SELECT id, uri, cid, label_value, error_text
        FROM label_publication
        WHERE status = 'failed'
        ORDER BY id DESC
        LIMIT 20
    """)).mappings().all()
    for row in rows:
        print(dict(row))

    print("\n=== latest pending rows ===")
    rows = conn.execute(text("""
        SELECT id, uri, cid, label_value
        FROM label_publication
        WHERE status = 'pending'
        ORDER BY id DESC
        LIMIT 20
    """)).mappings().all()
    for row in rows:
        print(dict(row))
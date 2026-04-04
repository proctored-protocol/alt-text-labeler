from sqlalchemy import text

from app.db import engine

DDL = """
CREATE TABLE IF NOT EXISTS label_visibility (
    id BIGSERIAL PRIMARY KEY,
    uri VARCHAR(512) NOT NULL,
    cid VARCHAR(128) NOT NULL,
    label_value VARCHAR(128) NOT NULL,

    first_published_at TIMESTAMPTZ NULL,
    record_created_at TIMESTAMPTZ NULL,

    forced_visible_at TIMESTAMPTZ NULL,
    subscriber_visible_at TIMESTAMPTZ NULL,

    last_forced_checked_at TIMESTAMPTZ NULL,
    last_subscriber_checked_at TIMESTAMPTZ NULL,

    last_forced_visible BOOLEAN NULL,
    last_subscriber_visible BOOLEAN NULL,

    forced_check_count INTEGER NOT NULL DEFAULT 0,
    subscriber_check_count INTEGER NOT NULL DEFAULT 0,

    last_forced_error TEXT NULL,
    last_subscriber_error TEXT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_label_visibility_triplet UNIQUE (uri, cid, label_value)
);

CREATE INDEX IF NOT EXISTS ix_label_visibility_first_published_at
    ON label_visibility (first_published_at);

CREATE INDEX IF NOT EXISTS ix_label_visibility_forced_visible_at
    ON label_visibility (forced_visible_at);

CREATE INDEX IF NOT EXISTS ix_label_visibility_subscriber_visible_at
    ON label_visibility (subscriber_visible_at);

CREATE INDEX IF NOT EXISTS ix_label_visibility_updated_at
    ON label_visibility (updated_at);
"""


def main() -> None:
    with engine.begin() as conn:
        for stmt in [s.strip() for s in DDL.split(";") if s.strip()]:
            conn.execute(text(stmt))
    print("label_visibility table ready")


if __name__ == "__main__":
    main()
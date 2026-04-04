from sqlalchemy import text

from app.db import engine

DDL = """
CREATE TABLE IF NOT EXISTS publish_job (
    id BIGSERIAL PRIMARY KEY,
    uri VARCHAR(512) NOT NULL,
    cid VARCHAR(128) NOT NULL,
    label_value VARCHAR(128) NOT NULL,
    state VARCHAR(32) NOT NULL DEFAULT 'pending',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    leased_until TIMESTAMPTZ NULL,
    leased_by VARCHAR(128) NULL,
    last_error TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_publish_job_triplet UNIQUE (uri, cid, label_value)
);

CREATE INDEX IF NOT EXISTS ix_publish_job_state_next_attempt
    ON publish_job (state, next_attempt_at);

CREATE INDEX IF NOT EXISTS ix_publish_job_leased_until
    ON publish_job (leased_until);

CREATE INDEX IF NOT EXISTS ix_publish_job_created_at
    ON publish_job (created_at);
"""


def main() -> None:
    with engine.begin() as conn:
        for stmt in [s.strip() for s in DDL.split(";") if s.strip()]:
            conn.execute(text(stmt))
    print("publish_job table ready")


if __name__ == "__main__":
    main()
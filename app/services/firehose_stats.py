from sqlalchemy import text
from sqlalchemy.orm import Session

COLS = [
    "commit_count",
    "post_create_count",
    "image_post_count",
    "image_eval_count",
    "missing_label_count",
    "partial_label_count",
    "publish_success_count",
    "publish_failed_count",
]


def bump_firehose_stats(session: Session, **increments: int) -> None:
    vals = {col: int(increments.get(col, 0)) for col in COLS}

    session.execute(
        text("""
            INSERT INTO firehose_minute_stats (
                bucket,
                commit_count,
                post_create_count,
                image_post_count,
                image_eval_count,
                missing_label_count,
                partial_label_count,
                publish_success_count,
                publish_failed_count
            ) VALUES (
                date_trunc('minute', now()),
                :commit_count,
                :post_create_count,
                :image_post_count,
                :image_eval_count,
                :missing_label_count,
                :partial_label_count,
                :publish_success_count,
                :publish_failed_count
            )
            ON CONFLICT (bucket) DO UPDATE SET
                commit_count = firehose_minute_stats.commit_count + EXCLUDED.commit_count,
                post_create_count = firehose_minute_stats.post_create_count + EXCLUDED.post_create_count,
                image_post_count = firehose_minute_stats.image_post_count + EXCLUDED.image_post_count,
                image_eval_count = firehose_minute_stats.image_eval_count + EXCLUDED.image_eval_count,
                missing_label_count = firehose_minute_stats.missing_label_count + EXCLUDED.missing_label_count,
                partial_label_count = firehose_minute_stats.partial_label_count + EXCLUDED.partial_label_count,
                publish_success_count = firehose_minute_stats.publish_success_count + EXCLUDED.publish_success_count,
                publish_failed_count = firehose_minute_stats.publish_failed_count + EXCLUDED.publish_failed_count
        """),
        vals,
    )
import logging

from atproto import (
    FirehoseSubscribeReposClient,
    models,
    parse_subscribe_repos_message,
)

from app.config import get_settings
from app.db import SessionLocal
from app.integrations.ozone.publisher import (
    enqueue_label_publication,
    publish_label_via_ozone,
)
from app.parsing.posts import iter_post_creates
from app.services.cursor import get_saved_cursor, save_cursor
from app.services.evaluator import evaluate_post, upsert_post_evaluation
from app.services.firehose_stats import bump_firehose_stats
from app.services.overrides import is_uri_suppressed

logger = logging.getLogger(__name__)


class FirehoseWorker:
    def __init__(self) -> None:
        self.settings = get_settings()

        cursor = self.settings.firehose_cursor
        with SessionLocal() as session:
            if cursor is None:
                cursor = get_saved_cursor(session)

        params = {"cursor": cursor} if cursor is not None else None

        self.client = FirehoseSubscribeReposClient(
            params=params,
            base_uri=self.settings.firehose_base_uri,
        )

        logger.info(
            "firehose_worker_initialized",
            extra={
                "base_uri": self.settings.firehose_base_uri,
                "cursor": cursor,
                "dry_run": self.settings.firehose_dry_run,
                "publish_via_ozone": self.settings.publish_via_ozone,
            },
        )

    def run(self) -> None:
        logger.info("firehose_worker_starting")
        self.client.start(self.on_message, self.on_callback_error)

    def on_callback_error(self, exc: BaseException) -> None:
        logger.exception("firehose_callback_error", exc_info=exc)

    def on_message(self, message) -> None:
        parsed = parse_subscribe_repos_message(message)
        if not isinstance(parsed, models.ComAtprotoSyncSubscribeRepos.Commit):
            return
        self._handle_commit(parsed)

    def _handle_commit(self, commit: models.ComAtprotoSyncSubscribeRepos.Commit) -> None:
        processed_posts = 0

        with SessionLocal() as session:
            # Count every commit the worker sees.
            bump_firehose_stats(session, commit_count=1)

            for post in iter_post_creates(commit):
                try:
                    # Count every created post the worker inspects.
                    bump_firehose_stats(session, post_create_count=1)

                    if not post.image_alts:
                        continue

                    # Count posts that have recognized still-image embeds.
                    bump_firehose_stats(session, image_post_count=1)

                    if is_uri_suppressed(session, post.uri):
                        logger.info("post_suppressed", extra={"uri": post.uri})
                        session.commit()
                        continue

                    result = evaluate_post(
                        post=post,
                        missing_label=self.settings.label_missing_alt,
                        partial_label=self.settings.label_partial_alt,
                        last_seen_seq=commit.seq,
                    )

                    if result is None:
                        session.commit()
                        continue

                    upsert_post_evaluation(session, result)
                    processed_posts += 1

                    # Count posts that made it through evaluation/persistence.
                    bump_firehose_stats(session, image_eval_count=1)

                    if result.derived_label == self.settings.label_missing_alt:
                        bump_firehose_stats(session, missing_label_count=1)
                    elif result.derived_label == self.settings.label_partial_alt:
                        bump_firehose_stats(session, partial_label_count=1)

                    logger.info(
                        "post_evaluated",
                        extra={
                            "uri": result.uri,
                            "cid": result.cid,
                            "image_count": result.image_count,
                            "usable_alt_count": result.usable_alt_count,
                            "derived_label": result.derived_label,
                            "dry_run": self.settings.firehose_dry_run,
                            "publish_via_ozone": self.settings.publish_via_ozone,
                        },
                    )

                    if result.derived_label:
                        if self.settings.firehose_dry_run or not self.settings.publish_via_ozone:
                            enqueue_label_publication(
                                session=session,
                                uri=result.uri,
                                cid=result.cid,
                                label_value=result.derived_label,
                            )
                            logger.info(
                                "dry_run_label_candidate",
                                extra={
                                    "uri": result.uri,
                                    "cid": result.cid,
                                    "label_value": result.derived_label,
                                },
                            )
                        else:
                            try:
                                publish_label_via_ozone(
                                    session=session,
                                    uri=result.uri,
                                    cid=result.cid,
                                    label_value=result.derived_label,
                                )
                                bump_firehose_stats(session, publish_success_count=1)
                                logger.info(
                                    "label_published_via_ozone",
                                    extra={
                                        "uri": result.uri,
                                        "cid": result.cid,
                                        "label_value": result.derived_label,
                                    },
                                )
                            except Exception:
                                bump_firehose_stats(session, publish_failed_count=1)
                                logger.exception(
                                    "label_publication_failed",
                                    extra={
                                        "repo": commit.repo,
                                        "seq": commit.seq,
                                        "uri": result.uri,
                                        "cid": result.cid,
                                        "label_value": result.derived_label,
                                    },
                                )

                    # Critical: persist evaluation rows and minute-bucket stats
                    # even if publication failed.
                    session.commit()

                except Exception:
                    session.rollback()
                    logger.exception(
                        "post_processing_failed",
                        extra={
                            "repo": commit.repo,
                            "seq": commit.seq,
                            "uri": getattr(post, "uri", None),
                        },
                    )

            try:
                save_cursor(session, commit.seq)
                session.commit()
            except Exception:
                session.rollback()
                logger.exception(
                    "cursor_save_failed",
                    extra={"repo": commit.repo, "seq": commit.seq},
                )
                return

        if processed_posts:
            logger.info(
                "commit_processed",
                extra={
                    "repo": commit.repo,
                    "seq": commit.seq,
                    "processed_posts": processed_posts,
                },
            )
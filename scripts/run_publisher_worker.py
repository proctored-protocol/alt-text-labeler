import argparse
import logging
import socket
import time

from app.config import get_settings
from app.db import SessionLocal, init_db
from app.integrations.ozone.publisher import publish_label_via_ozone
from app.logging import configure_logging
from app.services.firehose_stats import bump_firehose_stats
from app.services.publish_queue import (
    lease_publish_jobs,
    mark_publish_job_dead,
    mark_publish_job_published,
    mark_publish_job_retry,
    next_backoff_seconds,
)

logger = logging.getLogger(__name__)


def build_worker_id(arg_value: str | None) -> str:
    if arg_value:
        return arg_value
    return f"{socket.gethostname()}-publisher"


def main() -> None:
    parser = argparse.ArgumentParser(description="Drain publish_job queue and publish labels via Ozone")
    parser.add_argument("--worker-id", default=None)
    args = parser.parse_args()

    settings = get_settings()
    configure_logging(settings.log_level)
    init_db()

    worker_id = build_worker_id(args.worker_id)

    logger.info(
        "publisher_worker_starting",
        extra={
            "worker_id": worker_id,
            "batch_size": settings.publisher_batch_size,
            "lease_seconds": settings.publisher_lease_seconds,
            "max_attempts": settings.publisher_max_attempts,
            "backoff_base_seconds": settings.publisher_backoff_base_seconds,
        },
    )

    while True:
        with SessionLocal() as session:
            jobs = lease_publish_jobs(
                session,
                worker_id=worker_id,
                batch_size=settings.publisher_batch_size,
                lease_seconds=settings.publisher_lease_seconds,
            )
            session.commit()

        if not jobs:
            time.sleep(settings.publisher_idle_sleep_seconds)
            continue

        for job in jobs:
            try:
                with SessionLocal() as session:
                    publish_label_via_ozone(
                        session=session,
                        uri=job["uri"],
                        cid=job["cid"],
                        label_value=job["label_value"],
                    )
                    mark_publish_job_published(session=session, job_id=job["id"])
                    bump_firehose_stats(session, publish_success_count=1)
                    session.commit()

                logger.info(
                    "publish_job_succeeded",
                    extra={
                        "worker_id": worker_id,
                        "job_id": job["id"],
                        "uri": job["uri"],
                        "cid": job["cid"],
                        "label_value": job["label_value"],
                    },
                )

            except Exception as exc:
                attempt_after_failure = int(job["attempt_count"]) + 1
                error_text = str(exc)

                with SessionLocal() as session:
                    if attempt_after_failure >= settings.publisher_max_attempts:
                        mark_publish_job_dead(
                            session=session,
                            job_id=job["id"],
                            error_text=error_text,
                        )
                    else:
                        delay_seconds = next_backoff_seconds(
                            attempt_count_after_failure=attempt_after_failure,
                            base_seconds=settings.publisher_backoff_base_seconds,
                        )
                        mark_publish_job_retry(
                            session=session,
                            job_id=job["id"],
                            error_text=error_text,
                            delay_seconds=delay_seconds,
                        )

                    bump_firehose_stats(session, publish_failed_count=1)
                    session.commit()

                logger.exception(
                    "publish_job_failed",
                    extra={
                        "worker_id": worker_id,
                        "job_id": job["id"],
                        "uri": job["uri"],
                        "cid": job["cid"],
                        "label_value": job["label_value"],
                        "attempt_after_failure": attempt_after_failure,
                    },
                )


if __name__ == "__main__":
    main()
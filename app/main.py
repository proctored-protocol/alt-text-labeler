from fastapi import FastAPI

from app.api.health import router as health_router
from app.config import get_settings
from app.db import init_db
from app.firehose.client import FirehoseWorker
from app.logging import configure_logging

app = FastAPI(title="alt-labeler", version="0.1.0")
app.include_router(health_router)


def run() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    init_db()

    worker = FirehoseWorker()
    worker.run()


if __name__ == "__main__":
    run()
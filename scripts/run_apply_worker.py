from __future__ import annotations

import logging
import sys

from app.apply.worker import ApplyWorker
from app.config import get_settings


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stdout,
    )


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)

    worker = ApplyWorker()
    worker.run()


if __name__ == "__main__":
    main()
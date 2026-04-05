from __future__ import annotations

import logging
import sys

from app.config import get_settings
from app.visibility.worker import VisibilityWorker


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stdout,
    )


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)

    worker = VisibilityWorker()
    worker.run()


if __name__ == "__main__":
    main()
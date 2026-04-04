from app.config import get_settings
from app.db import init_db
from app.firehose.client import FirehoseWorker
from app.logging import configure_logging


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    init_db()
    FirehoseWorker().run()


if __name__ == "__main__":
    main()
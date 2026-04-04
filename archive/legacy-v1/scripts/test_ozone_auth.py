from pprint import pprint

from app.integrations.ozone.client import get_server_config


def main() -> None:
    config = get_server_config()
    pprint(config)


if __name__ == "__main__":
    main()
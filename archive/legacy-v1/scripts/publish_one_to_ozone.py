import sys

from app.db import SessionLocal, init_db
from app.integrations.ozone.publisher import publish_label_via_ozone


def main() -> None:
    if len(sys.argv) != 4:
        print("Usage: python scripts/publish_one_to_ozone.py <uri> <cid> <label_value>")
        raise SystemExit(1)

    uri = sys.argv[1]
    cid = sys.argv[2]
    label_value = sys.argv[3]

    init_db()

    with SessionLocal() as session:
        try:
            row = publish_label_via_ozone(
                session=session,
                uri=uri,
                cid=cid,
                label_value=label_value,
            )
            session.commit()
            print("Published label via Ozone:")
            print(f"  uri: {row.uri}")
            print(f"  cid: {row.cid}")
            print(f"  label: {row.label_value}")
            print(f"  status: {row.status}")
            print(f"  published_at: {row.published_at}")
        except Exception:
            session.rollback()
            raise


if __name__ == "__main__":
    main()
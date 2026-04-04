from sqlalchemy import func, select

from app.db import SessionLocal
from app.models import LabelPublication


def main() -> None:
    with SessionLocal() as session:
        total_stmt = select(func.count()).select_from(LabelPublication)
        total = session.execute(total_stmt).scalar_one()

        grouped_stmt = (
            select(LabelPublication.status, func.count())
            .group_by(LabelPublication.status)
            .order_by(LabelPublication.status)
        )
        grouped = session.execute(grouped_stmt).all()

    print(f"Total publication rows: {total}")
    print()
    print("Counts by status:")
    for status, count in grouped:
        print(f"  {status!r}: {count}")


if __name__ == "__main__":
    main()
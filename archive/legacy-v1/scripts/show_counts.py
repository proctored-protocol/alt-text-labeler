from sqlalchemy import func, select

from app.db import SessionLocal
from app.models import PostEvaluation


def main() -> None:
    with SessionLocal() as session:
        total_stmt = select(func.count()).select_from(PostEvaluation)
        total = session.execute(total_stmt).scalar_one()

        grouped_stmt = (
            select(PostEvaluation.derived_label, func.count())
            .group_by(PostEvaluation.derived_label)
            .order_by(PostEvaluation.derived_label)
        )
        grouped = session.execute(grouped_stmt).all()

    print(f"Total evaluations: {total}")
    print()
    print("Counts by label:")
    for label, count in grouped:
        print(f"  {label!r}: {count}")


if __name__ == "__main__":
    main()
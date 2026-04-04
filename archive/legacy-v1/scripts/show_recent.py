from sqlalchemy import select

from app.db import SessionLocal
from app.models import PostEvaluation


def main(limit: int = 20) -> None:
    with SessionLocal() as session:
        stmt = (
            select(PostEvaluation)
            .order_by(PostEvaluation.evaluated_at.desc())
            .limit(limit)
        )
        rows = session.execute(stmt).scalars().all()

    if not rows:
        print("No evaluations found.")
        return

    for row in rows:
        print("-" * 80)
        print(f"URI:              {row.uri}")
        print(f"CID:              {row.cid}")
        print(f"Author DID:       {row.author_did}")
        print(f"Images:           {row.image_count}")
        print(f"Usable alts:      {row.usable_alt_count}")
        print(f"Derived label:    {row.derived_label}")
        print(f"Embed type:       {row.raw_embed_type}")
        print(f"Created at:       {row.record_created_at}")
        print(f"Evaluated at:     {row.evaluated_at}")
        print(f"Last seen seq:    {row.last_seen_seq}")


if __name__ == "__main__":
    main()
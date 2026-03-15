from sqlalchemy.orm import Session

from app.models import PostEvaluation
from app.rules.labeling import derive_post_label
from app.schemas import EvaluationResult, ParsedPostCreate


def evaluate_post(
    post: ParsedPostCreate,
    missing_label: str,
    partial_label: str,
    last_seen_seq: int | None = None,
) -> EvaluationResult | None:
    if not post.image_alts:
        return None

    image_count, usable_alt_count, derived_label = derive_post_label(
        image_alts=post.image_alts,
        missing_label=missing_label,
        partial_label=partial_label,
    )

    return EvaluationResult(
        uri=post.uri,
        cid=post.cid,
        author_did=post.author_did,
        repo_did=post.repo_did,
        image_count=image_count,
        usable_alt_count=usable_alt_count,
        derived_label=derived_label,
        record_created_at=post.created_at,
        raw_embed_type=post.raw_embed_type,
        last_seen_seq=last_seen_seq,
    )


def upsert_post_evaluation(session: Session, result: EvaluationResult) -> PostEvaluation:
    row = session.get(PostEvaluation, result.uri)

    if row is None:
        row = PostEvaluation(
            uri=result.uri,
            cid=result.cid,
            author_did=result.author_did,
            repo_did=result.repo_did,
            image_count=result.image_count,
            usable_alt_count=result.usable_alt_count,
            derived_label=result.derived_label,
            record_created_at=result.record_created_at,
            raw_embed_type=result.raw_embed_type,
            last_seen_seq=result.last_seen_seq,
        )
        session.add(row)
        return row

    row.cid = result.cid
    row.author_did = result.author_did
    row.repo_did = result.repo_did
    row.image_count = result.image_count
    row.usable_alt_count = result.usable_alt_count
    row.derived_label = result.derived_label
    row.record_created_at = result.record_created_at
    row.raw_embed_type = result.raw_embed_type
    row.last_seen_seq = result.last_seen_seq
    return row
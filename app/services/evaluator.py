from sqlalchemy import text
from sqlalchemy.orm import Session

from app.models import PostEvaluation
from app.rules.labeling import derive_post_label
from app.schemas import EvaluationResult, ParsedPostCreate


UPSERT_POST_EVALUATION_SQL = text(
    """
    INSERT INTO post_evaluation (
        uri,
        cid,
        author_did,
        repo_did,
        image_count,
        usable_alt_count,
        derived_label,
        record_created_at,
        raw_embed_type,
        evaluated_at,
        last_seen_seq
    ) VALUES (
        :uri,
        :cid,
        :author_did,
        :repo_did,
        :image_count,
        :usable_alt_count,
        :derived_label,
        :record_created_at,
        :raw_embed_type,
        NOW(),
        :last_seen_seq
    )
    ON CONFLICT (uri) DO UPDATE SET
        cid = EXCLUDED.cid,
        author_did = EXCLUDED.author_did,
        repo_did = EXCLUDED.repo_did,
        image_count = EXCLUDED.image_count,
        usable_alt_count = EXCLUDED.usable_alt_count,
        derived_label = EXCLUDED.derived_label,
        record_created_at = EXCLUDED.record_created_at,
        raw_embed_type = EXCLUDED.raw_embed_type,
        evaluated_at = NOW(),
        last_seen_seq = EXCLUDED.last_seen_seq
    """
)


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


def _result_to_mapping(result: EvaluationResult) -> dict:
    return {
        "uri": result.uri,
        "cid": result.cid,
        "author_did": result.author_did,
        "repo_did": result.repo_did,
        "image_count": result.image_count,
        "usable_alt_count": result.usable_alt_count,
        "derived_label": result.derived_label,
        "record_created_at": result.record_created_at,
        "raw_embed_type": result.raw_embed_type,
        "last_seen_seq": result.last_seen_seq,
    }


def upsert_post_evaluation(session: Session, result: EvaluationResult) -> None:
    session.execute(UPSERT_POST_EVALUATION_SQL, _result_to_mapping(result))


def upsert_post_evaluations(session: Session, results: list[EvaluationResult]) -> None:
    if not results:
        return

    session.execute(
        UPSERT_POST_EVALUATION_SQL,
        [_result_to_mapping(result) for result in results],
    )
from app.schemas import ParsedPostCreate
from app.services.evaluator import evaluate_post


def test_evaluate_post_returns_none_for_non_image_post() -> None:
    post = ParsedPostCreate(
        uri="at://did:plc:test/app.bsky.feed.post/abc",
        cid="cid1",
        repo_did="did:plc:test",
        author_did="did:plc:test",
        path="app.bsky.feed.post/abc",
        created_at="2026-03-13T00:00:00.000Z",
        image_alts=[],
        raw_record={},
        raw_embed_type=None,
    )

    result = evaluate_post(
        post=post,
        missing_label="missing-alt-text",
        partial_label="partial-alt-text",
        last_seen_seq=123,
    )

    assert result is None


def test_evaluate_post_sets_missing_label() -> None:
    post = ParsedPostCreate(
        uri="at://did:plc:test/app.bsky.feed.post/abc",
        cid="cid1",
        repo_did="did:plc:test",
        author_did="did:plc:test",
        path="app.bsky.feed.post/abc",
        created_at="2026-03-13T00:00:00.000Z",
        image_alts=[None],
        raw_record={},
        raw_embed_type="app.bsky.embed.images",
    )

    result = evaluate_post(
        post=post,
        missing_label="missing-alt-text",
        partial_label="partial-alt-text",
        last_seen_seq=123,
    )

    assert result is not None
    assert result.image_count == 1
    assert result.usable_alt_count == 0
    assert result.derived_label == "missing-alt-text"
    assert result.last_seen_seq == 123
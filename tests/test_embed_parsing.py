from app.parsing.embeds import extract_image_alts_from_record


def test_extract_direct_images_embed() -> None:
    record = {
        "$type": "app.bsky.feed.post",
        "text": "hello",
        "embed": {
            "$type": "app.bsky.embed.images",
            "images": [
                {"alt": "a cat"},
                {"alt": None},
            ],
        },
    }

    assert extract_image_alts_from_record(record) == ["a cat", None]


def test_extract_record_with_media_images_embed() -> None:
    record = {
        "$type": "app.bsky.feed.post",
        "text": "hello",
        "embed": {
            "$type": "app.bsky.embed.recordWithMedia",
            "record": {
                "$type": "app.bsky.embed.record",
                "record": {"uri": "at://did:plc:test/app.bsky.feed.post/abc", "cid": "bafyreitest"},
            },
            "media": {
                "$type": "app.bsky.embed.images",
                "images": [
                    {"alt": "chart screenshot"},
                    {"alt": "   "},
                ],
            },
        },
    }

    assert extract_image_alts_from_record(record) == ["chart screenshot", "   "]


def test_non_image_embed_returns_empty_list() -> None:
    record = {
        "$type": "app.bsky.feed.post",
        "text": "hello",
        "embed": {
            "$type": "app.bsky.embed.external",
            "external": {"uri": "https://example.com"},
        },
    }

    assert extract_image_alts_from_record(record) == []
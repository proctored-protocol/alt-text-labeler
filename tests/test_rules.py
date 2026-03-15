from app.rules.alt_text import is_usable_alt_text
from app.rules.labeling import derive_post_label


def test_whitespace_alt_is_not_usable() -> None:
    assert is_usable_alt_text("   ") is False


def test_single_image_missing_alt_gets_missing_label() -> None:
    image_count, usable_alt_count, label = derive_post_label(
        image_alts=[None],
        missing_label="missing-alt-text",
        partial_label="partial-alt-text",
    )

    assert image_count == 1
    assert usable_alt_count == 0
    assert label == "missing-alt-text"


def test_multi_image_partial_alt_gets_partial_label() -> None:
    image_count, usable_alt_count, label = derive_post_label(
        image_alts=["a cat", None, "a chart"],
        missing_label="missing-alt-text",
        partial_label="partial-alt-text",
    )

    assert image_count == 3
    assert usable_alt_count == 2
    assert label == "partial-alt-text"


def test_all_images_with_alt_get_no_label() -> None:
    image_count, usable_alt_count, label = derive_post_label(
        image_alts=["a cat", "a dog"],
        missing_label="missing-alt-text",
        partial_label="partial-alt-text",
    )

    assert image_count == 2
    assert usable_alt_count == 2
    assert label is None
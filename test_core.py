import pytest
from core import normalize_openalex_id, canonical_key, classify_review_preprint

def test_normalize_openalex_id():
    assert normalize_openalex_id("https://openalex.org/W123") == "W123"
    assert normalize_openalex_id("W999") == "W999"
    with pytest.raises(ValueError):
        normalize_openalex_id("X123")

def test_classify_review_preprint():
    r, p = classify_review_preprint("A systematic review of X", None, None, "Nature")
    assert r is True
    assert p is False
    r, p = classify_review_preprint("Foo", "preprint", None, "bioRxiv")
    assert p is True

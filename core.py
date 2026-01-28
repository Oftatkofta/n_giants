from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, List, Dict, Any, Tuple
import re

PREPRINT_VENUE_PAT = re.compile(r"\b(arxiv|biorxiv|medrxiv|chemrxiv|ssrn|research square|preprints\.org)\b", re.I)
REVIEW_TITLE_PAT = re.compile(r"\b(review|systematic review|meta-analysis|survey|tutorial)\b", re.I)
DOI_CLEAN_PAT = re.compile(r"^(?:https?://doi\.org/|doi:)", re.I)

def norm_doi(x: str) -> str:
    return DOI_CLEAN_PAT.sub("", x.strip()).strip().lower()

def normalize_openalex_id(x: str) -> str:
    # Accept W123... or https://openalex.org/W123...
    x = x.strip()
    x = x.replace("https://openalex.org/", "")
    if not x.startswith("W"):
        raise ValueError(f"Not an OpenAlex work id: {x}")
    return x

def canonical_key(doi: Optional[str], oa_id: Optional[str], s2_id: Optional[str]) -> str:
    if doi:
        return f"doi:{norm_doi(doi)}"
    if oa_id:
        return f"openalex:{normalize_openalex_id(oa_id)}"
    if s2_id:
        return f"s2:{s2_id}"
    raise ValueError("No identifier available to build canonical key")

def classify_review_preprint(
    title: Optional[str],
    oa_type: Optional[str],
    s2_pub_types: Optional[list[str]],
    venue: Optional[str],
) -> Tuple[bool, bool]:
    is_review = (oa_type == "review")
    is_preprint = (oa_type == "preprint")

    if s2_pub_types:
        lowered = {t.lower() for t in s2_pub_types if isinstance(t, str)}
        if "review" in lowered:
            is_review = True
        if "preprint" in lowered:
            is_preprint = True

    if not is_review and title and REVIEW_TITLE_PAT.search(title):
        is_review = True

    if not is_preprint and venue and PREPRINT_VENUE_PAT.search(venue):
        is_preprint = True

    return is_review, is_preprint

@dataclass
class WorkNode:
    key: str
    doi: Optional[str] = None
    oa_id: Optional[str] = None
    s2_id: Optional[str] = None
    title: Optional[str] = None
    year: Optional[int] = None
    oa_type: Optional[str] = None
    venue: Optional[str] = None
    is_review: bool = False
    is_preprint: bool = False
    refs: Optional[list[str]] = None      # canonical keys or openalex:W... placeholders
    refs_source: Optional[str] = None     # "openalex" | "s2" | "none"

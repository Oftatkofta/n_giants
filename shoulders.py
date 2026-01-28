#!/usr/bin/env python3
"""
shoulders.py — Count unique "shoulders" (backward citation closure) for a seed paper.

Features
- OpenAlex-first traversal of referenced_works
- Semantic Scholar fallback if OpenAlex has no references
- Exclude reviews and preprints (do not count, do not expand)
- Include books / book chapters as normal works
- SQLite cache for reproducibility and speed
- Emits a defensible coverage report

Usage
  python shoulders.py --doi 10.1128/mbio.00022-22 --max-depth 20

Optional env vars
  OPENALEX_API_KEY=...          (strongly recommended; OpenAlex will require it soon)
  OPENALEX_MAILTO=you@domain    (optional, polite pool / identification)
  SEMANTIC_SCHOLAR_API_KEY=...  (optional; improves reliability; see S2 docs)

"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
import time
from dotenv import load_dotenv  
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import requests

load_dotenv()
OPENALEX_BASE = "https://api.openalex.org"
S2_BASE = "https://api.semanticscholar.org/graph/v1"

# Repository / preprint venue heuristics (fallback)
PREPRINT_VENUE_PAT = re.compile(
    r"\b(arxiv|biorxiv|medrxiv|chemrxiv|ssrn|research square|preprints\.org)\b",
    re.IGNORECASE,
)
REVIEW_TITLE_PAT = re.compile(
    r"\b(review|systematic review|meta-analysis|survey|tutorial)\b",
    re.IGNORECASE,
)

DOI_CLEAN_PAT = re.compile(r"^(?:https?://doi\.org/|doi:)", re.IGNORECASE)


def norm_doi(x: str) -> str:
    x = x.strip()
    x = DOI_CLEAN_PAT.sub("", x)
    x = x.strip().lower()
    return x


def chunks(seq: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


@dataclass
class Work:
    key: str                   # canonical: doi:<doi> | openalex:<id> | s2:<id>
    doi: Optional[str]
    title: Optional[str]
    year: Optional[int]
    oa_id: Optional[str]
    s2_id: Optional[str]
    oa_type: Optional[str]     # OpenAlex "type" when available
    s2_pub_types: Optional[str]  # JSON string of publicationTypes
    venue: Optional[str]       # best-effort
    is_review: int
    is_preprint: int
    refs: List[str]            # canonical keys (or OA IDs if not yet resolved)
    refs_source: Optional[str] # "openalex"|"s2"|None


class Cache:
    def __init__(self, path: str):
        self.conn = sqlite3.connect(path)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self._init_schema()

    def _init_schema(self):
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS works (
              key TEXT PRIMARY KEY,
              doi TEXT,
              title TEXT,
              year INTEGER,
              oa_id TEXT,
              s2_id TEXT,
              oa_type TEXT,
              s2_pub_types TEXT,
              venue TEXT,
              is_review INTEGER,
              is_preprint INTEGER,
              refs_json TEXT,
              refs_source TEXT,
              fetched_at INTEGER
            );
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS frontier (
              seed_key TEXT,
              key TEXT,
              depth INTEGER,
              PRIMARY KEY(seed_key, key)
            );
            """
        )
        self.conn.commit()

    def get_work(self, key: str) -> Optional[Work]:
        row = self.conn.execute(
            "SELECT key,doi,title,year,oa_id,s2_id,oa_type,s2_pub_types,venue,is_review,is_preprint,refs_json,refs_source "
            "FROM works WHERE key=?",
            (key,),
        ).fetchone()
        if not row:
            return None
        refs = json.loads(row[11]) if row[11] else []
        return Work(
            key=row[0], doi=row[1], title=row[2], year=row[3], oa_id=row[4], s2_id=row[5],
            oa_type=row[6], s2_pub_types=row[7], venue=row[8], is_review=row[9], is_preprint=row[10],
            refs=refs, refs_source=row[12]
        )

    def upsert_work(self, w: Work):
        self.conn.execute(
            """
            INSERT INTO works (key,doi,title,year,oa_id,s2_id,oa_type,s2_pub_types,venue,is_review,is_preprint,refs_json,refs_source,fetched_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(key) DO UPDATE SET
              doi=excluded.doi,
              title=excluded.title,
              year=excluded.year,
              oa_id=excluded.oa_id,
              s2_id=excluded.s2_id,
              oa_type=excluded.oa_type,
              s2_pub_types=excluded.s2_pub_types,
              venue=excluded.venue,
              is_review=excluded.is_review,
              is_preprint=excluded.is_preprint,
              refs_json=excluded.refs_json,
              refs_source=excluded.refs_source,
              fetched_at=excluded.fetched_at;
            """,
            (
                w.key, w.doi, w.title, w.year, w.oa_id, w.s2_id, w.oa_type, w.s2_pub_types, w.venue,
                w.is_review, w.is_preprint, json.dumps(w.refs), w.refs_source, int(time.time())
            ),
        )
        self.conn.commit()

    def set_frontier(self, seed_key: str, key: str, depth: int):
        self.conn.execute(
            "INSERT OR IGNORE INTO frontier(seed_key,key,depth) VALUES (?,?,?)",
            (seed_key, key, depth),
        )
        self.conn.commit()

    def get_frontier(self, seed_key: str) -> List[Tuple[str, int]]:
        rows = self.conn.execute(
            "SELECT key, depth FROM frontier WHERE seed_key=? ORDER BY depth ASC",
            (seed_key,),
        ).fetchall()
        return [(r[0], r[1]) for r in rows]


class OpenAlexClient:
    def __init__(self):
        self.api_key = os.getenv("OPENALEX_API_KEY")
        self.mailto = os.getenv("OPENALEX_MAILTO")
        self.sess = requests.Session()

    def _params(self, extra: Dict[str, Any]) -> Dict[str, Any]:
        p = dict(extra)
        if self.api_key:
            p["api_key"] = self.api_key  # docs specify api_key=... :contentReference[oaicite:2]{index=2}
        if self.mailto:
            p["mailto"] = self.mailto
        return p

    def resolve_doi(self, doi: str) -> Optional[Dict[str, Any]]:
        r = self.sess.get(
            f"{OPENALEX_BASE}/works",
            params=self._params({"filter": f"doi:{doi}", "per-page": 1}),
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        results = data.get("results") or []
        return results[0] if results else None

    def get_works_by_ids(self, oa_ids: List[str]) -> List[Dict[str, Any]]:
        # Batch fetch using OR filter (id:...|...); keep fields small with select
        out: List[Dict[str, Any]] = []
        for batch in chunks(oa_ids, 50):
            filt = "id:" + "|".join(batch)
            r = self.sess.get(
                f"{OPENALEX_BASE}/works",
                params=self._params({
                    "filter": filt,
                    "per-page": 200,
                    "select": "id,doi,title,publication_year,type,host_venue,primary_location,referenced_works"
                }),
                timeout=60,
            )
            r.raise_for_status()
            data = r.json()
            out.extend(data.get("results") or [])
        return out


class SemanticScholarClient:
    def __init__(self):
        self.api_key = os.getenv("SEMANTIC_SCHOLAR_API_KEY")
        self.sess = requests.Session()
        if self.api_key:
            self.sess.headers.update({"x-api-key": self.api_key})

    def get_paper_by_doi(self, doi: str) -> Optional[Dict[str, Any]]:
        fields = "paperId,externalIds,title,year,venue,publicationTypes"
        r = self.sess.get(
            f"{S2_BASE}/paper/DOI:{doi}",
            params={"fields": fields},
            timeout=30,
        )
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()

    def get_references(self, paper_id: str) -> Optional[List[Dict[str, Any]]]:
        # Dedicated references endpoint is documented. :contentReference[oaicite:3]{index=3}
        fields = "references.paperId,references.externalIds,references.title,references.year,references.venue,references.publicationTypes"
        r = self.sess.get(
            f"{S2_BASE}/paper/{paper_id}/references",
            params={"fields": fields, "limit": 1000},
            timeout=60,
        )
        if r.status_code == 404:
            return None
        r.raise_for_status()
        data = r.json()
        # Response typically includes "data": [{"citedPaper": {...}}, ...]
        items = data.get("data") or []
        refs = []
        for it in items:
            cp = it.get("citedPaper")
            if cp:
                refs.append(cp)
        return refs


def classify_review_preprint(
    title: Optional[str],
    oa_type: Optional[str],
    s2_pub_types: Optional[List[str]],
    venue: Optional[str],
) -> Tuple[int, int]:
    """Return (is_review, is_preprint) as ints 0/1."""
    t = (title or "").strip()

    # OpenAlex types include books and other work classes; treat review/preprint explicitly. :contentReference[oaicite:4]{index=4}
    is_review = 1 if (oa_type == "review") else 0
    is_preprint = 1 if (oa_type == "preprint") else 0

    # Semantic Scholar publicationTypes can contain "Review" / "Preprint" depending on record.
    if s2_pub_types:
        lowered = {x.lower() for x in s2_pub_types if isinstance(x, str)}
        if "review" in lowered:
            is_review = 1
        if "preprint" in lowered:
            is_preprint = 1

    # Heuristic fallback signals
    if not is_review and REVIEW_TITLE_PAT.search(t):
        is_review = 1

    v = (venue or "")
    if not is_preprint and PREPRINT_VENUE_PAT.search(v):
        is_preprint = 1

    return is_review, is_preprint


def canonical_key(doi: Optional[str], oa_id: Optional[str], s2_id: Optional[str]) -> str:
    if doi:
        return f"doi:{norm_doi(doi)}"
    if oa_id:
        return f"openalex:{oa_id}"
    if s2_id:
        return f"s2:{s2_id}"
    # Should not happen in practice; caller should avoid creating keyless works
    return "unknown:" + str(hash((doi, oa_id, s2_id)))


def oa_extract_venue(rec: Dict[str, Any]) -> Optional[str]:
    # best-effort: host_venue.display_name or primary_location.source.display_name
    hv = rec.get("host_venue") or {}
    if isinstance(hv, dict):
        dn = hv.get("display_name")
        if dn:
            return dn
    pl = rec.get("primary_location") or {}
    if isinstance(pl, dict):
        src = pl.get("source") or {}
        if isinstance(src, dict):
            dn = src.get("display_name")
            if dn:
                return dn
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--doi", required=True, help="Seed DOI (e.g., 10.1128/mbio.00022-22)")
    ap.add_argument("--db", default="shoulders_cache.sqlite", help="SQLite cache path")
    ap.add_argument("--max-depth", type=int, default=20)
    ap.add_argument("--min-year", type=int, default=0, help="Stop expanding works older than this year (0 disables)")
    ap.add_argument("--max-nodes", type=int, default=200000, help="Hard cap on counted nodes (safety)")
    ap.add_argument("--sleep", type=float, default=0.0, help="Optional sleep between batches (seconds)")
    args = ap.parse_args()

    doi = norm_doi(args.doi)

    cache = Cache(args.db)
    oa = OpenAlexClient()
    s2 = SemanticScholarClient()

    # Resolve seed via OpenAlex first (preferred because referenced_works is native). :contentReference[oaicite:5]{index=5}
    seed_rec = oa.resolve_doi(doi)
    seed_oa_id = seed_rec.get("id") if seed_rec else None
    seed_doi = seed_rec.get("doi") if seed_rec else doi
    seed_title = seed_rec.get("title") if seed_rec else None
    seed_year = seed_rec.get("publication_year") if seed_rec else None
    seed_type = seed_rec.get("type") if seed_rec else None
    seed_venue = oa_extract_venue(seed_rec) if seed_rec else None

    seed_s2 = None
    seed_s2_id = None
    seed_s2_pub_types = None
    if not seed_rec:
        seed_s2 = s2.get_paper_by_doi(doi)
        if not seed_s2:
            print(f"Could not resolve seed DOI in OpenAlex or Semantic Scholar: {doi}", file=sys.stderr)
            sys.exit(2)
        seed_s2_id = seed_s2.get("paperId")
        seed_s2_pub_types = seed_s2.get("publicationTypes") or None
        seed_title = seed_s2.get("title") or seed_title
        seed_year = seed_s2.get("year") or seed_year
        seed_venue = seed_s2.get("venue") or seed_venue

    seed_is_review, seed_is_preprint = classify_review_preprint(
        seed_title, seed_type, seed_s2_pub_types, seed_venue
    )
    seed_key = canonical_key(seed_doi, seed_oa_id, seed_s2_id)

    if seed_is_review:
        print("Seed paper classified as REVIEW; per your rules it would be excluded.", file=sys.stderr)
        sys.exit(2)
    if seed_is_preprint:
        print("Seed paper classified as PREPRINT; per your rules it would be excluded.", file=sys.stderr)
        sys.exit(2)

    seed_work = Work(
        key=seed_key,
        doi=seed_doi,
        title=seed_title,
        year=int(seed_year) if seed_year else None,
        oa_id=seed_oa_id,
        s2_id=seed_s2_id,
        oa_type=seed_type,
        s2_pub_types=json.dumps(seed_s2_pub_types) if seed_s2_pub_types else None,
        venue=seed_venue,
        is_review=seed_is_review,
        is_preprint=seed_is_preprint,
        refs=[],
        refs_source=None,
    )
    cache.upsert_work(seed_work)

    # BFS frontier: queue holds keys to expand; counted holds included (non-review, non-preprint)
    to_expand: List[Tuple[str, int]] = [(seed_key, 0)]
    seen: Set[str] = set([seed_key])

    # Metrics
    counted: Set[str] = set()  # all counted "shoulders" (excluding seed? we can exclude seed)
    excluded_reviews = 0
    excluded_preprints = 0
    terminal_no_refs = 0
    used_oa_refs = 0
    used_s2_refs = 0
    earliest_year: Optional[int] = None
    max_depth_reached = 0

    # We typically do NOT count the seed itself as a "shoulder"
    # counted.add(seed_key)  # keep commented out

    while to_expand:
        # Expand in small batches for OpenAlex efficiency
        batch: List[Tuple[str, int]] = []
        while to_expand and len(batch) < 50:
            batch.append(to_expand.pop(0))
        max_depth_reached = max(max_depth_reached, max(d for _, d in batch))

        # Identify which of these have OA IDs and are not cached with refs yet
        oa_ids_need: List[str] = []
        key_by_oa: Dict[str, str] = {}
        depth_by_key: Dict[str, int] = {}
        for k, d in batch:
            depth_by_key[k] = d
            w = cache.get_work(k)
            if w and w.refs_source is not None:
                continue  # already expanded
            if w and w.oa_id:
                oa_ids_need.append(w.oa_id)
                key_by_oa[w.oa_id] = k

        # Fetch OA records for the batch keys that have OA IDs
        oa_recs = oa.get_works_by_ids(oa_ids_need) if oa_ids_need else []
        oa_rec_by_id = {r.get("id"): r for r in oa_recs if r.get("id")}

        # Expand each work in batch
        for k, depth in batch:
            if depth >= args.max_depth:
                continue

            w = cache.get_work(k)
            if not w:
                continue

            # Year cutoff: do not expand beyond threshold
            if args.min_year and w.year and w.year < args.min_year:
                # don't expand older works
                cache.upsert_work(Work(**{**w.__dict__, "refs": [], "refs_source": "year_cutoff"}))
                continue

            refs_canonical: List[str] = []
            refs_source: Optional[str] = None

            # Try OpenAlex references first
            if w.oa_id and w.oa_id in oa_rec_by_id:
                rec = oa_rec_by_id[w.oa_id]
                ref_oa_ids = rec.get("referenced_works") or []
                if ref_oa_ids:
                    refs_source = "openalex"
                    used_oa_refs += 1
                    # We first store OA IDs; we will resolve them into canonical keys below.
                    # We'll resolve by fetching those OA IDs in later iterations, which will give DOIs/books/etc.
                    for rid in ref_oa_ids:
                        if isinstance(rid, str) and rid.startswith("https://openalex.org/"):
                            refs_canonical.append(f"openalex:{rid}")
                        elif isinstance(rid, str):
                            refs_canonical.append(f"openalex:{rid}")
                else:
                    # empty in OpenAlex; fallback to S2
                    pass

                # Update metadata for current node from OA record (more complete)
                w_title = rec.get("title") or w.title
                w_year = rec.get("publication_year") or w.year
                w_doi = rec.get("doi") or w.doi
                w_type = rec.get("type") or w.oa_type
                w_venue = oa_extract_venue(rec) or w.venue
                is_rev, is_pre = classify_review_preprint(w_title, w_type, None, w_venue)
                w = Work(
                    key=w.key, doi=w_doi, title=w_title,
                    year=int(w_year) if w_year else None,
                    oa_id=w.oa_id, s2_id=w.s2_id,
                    oa_type=w_type, s2_pub_types=w.s2_pub_types,
                    venue=w_venue, is_review=is_rev, is_preprint=is_pre,
                    refs=w.refs, refs_source=w.refs_source
                )

            # If no OA refs, fallback to Semantic Scholar refs (only when needed)
            if refs_source is None:
                # Need S2 paper id. If missing, try DOI lookup first.
                s2_id = w.s2_id
                s2_pub_types = None
                if not s2_id and w.doi:
                    s2_p = s2.get_paper_by_doi(norm_doi(w.doi))
                    if s2_p:
                        s2_id = s2_p.get("paperId")
                        s2_pub_types = s2_p.get("publicationTypes") or None
                        # Update classification using S2 info
                        is_rev, is_pre = classify_review_preprint(w.title, w.oa_type, s2_pub_types, w.venue)
                        w.s2_id = s2_id
                        w.s2_pub_types = json.dumps(s2_pub_types) if s2_pub_types else w.s2_pub_types
                        w.is_review = is_rev
                        w.is_preprint = is_pre

                if s2_id:
                    refs = s2.get_references(s2_id)
                    if refs:
                        refs_source = "s2"
                        used_s2_refs += 1
                        for cp in refs:
                            ext = cp.get("externalIds") or {}
                            cp_doi = ext.get("DOI") if isinstance(ext, dict) else None
                            cp_id = cp.get("paperId")
                            cp_title = cp.get("title")
                            cp_year = cp.get("year")
                            cp_venue = cp.get("venue")
                            cp_pub_types = cp.get("publicationTypes") or None

                            is_rev, is_pre = classify_review_preprint(cp_title, None, cp_pub_types, cp_venue)
                            if is_rev:
                                excluded_reviews += 1
                                continue
                            if is_pre:
                                excluded_preprints += 1
                                continue

                            ck = canonical_key(cp_doi, None, cp_id)
                            refs_canonical.append(ck)

                            # Upsert minimal metadata for referenced work so it can be expanded later
                            cw = Work(
                                key=ck,
                                doi=cp_doi.lower() if isinstance(cp_doi, str) else None,
                                title=cp_title,
                                year=int(cp_year) if cp_year else None,
                                oa_id=None,
                                s2_id=cp_id,
                                oa_type=None,
                                s2_pub_types=json.dumps(cp_pub_types) if cp_pub_types else None,
                                venue=cp_venue,
                                is_review=is_rev,
                                is_preprint=is_pre,
                                refs=[],
                                refs_source=None,
                            )
                            cache.upsert_work(cw)

            # If still no refs, mark terminal
            if refs_source is None:
                terminal_no_refs += 1
                cache.upsert_work(Work(**{**w.__dict__, "refs": [], "refs_source": None}))
                continue

            # Store refs for this node
            w.refs = refs_canonical
            w.refs_source = refs_source
            cache.upsert_work(w)

            # Process referenced works into frontier
            next_depth = depth + 1
            for rk in refs_canonical:
                # If OA ref key is openalex:<id>, normalize (strip to OA id string)
                oa_id = None
                if rk.startswith("openalex:"):
                    oa_id = rk.replace("openalex:", "", 1)

                # If it's an OA ref, create a placeholder work so it can be fetched in OA batch later
                if oa_id and oa_id.startswith("https://openalex.org/"):
                    # OpenAlex IDs are URLs already; keep as-is.
                    placeholder_key = f"openalex:{oa_id}"
                    if placeholder_key not in seen:
                        seen.add(placeholder_key)
                        pw = Work(
                            key=placeholder_key, doi=None, title=None, year=None,
                            oa_id=oa_id, s2_id=None, oa_type=None, s2_pub_types=None,
                            venue=None, is_review=0, is_preprint=0,
                            refs=[], refs_source=None
                        )
                        cache.upsert_work(pw)
                        to_expand.append((placeholder_key, next_depth))
                        cache.set_frontier(seed_key, placeholder_key, next_depth)
                    continue

                # For DOI/S2-keyed refs
                if rk not in seen:
                    # If work not in cache yet, create minimal placeholder
                    if not cache.get_work(rk):
                        pw = Work(
                            key=rk, doi=None, title=None, year=None,
                            oa_id=None, s2_id=None, oa_type=None, s2_pub_types=None,
                            venue=None, is_review=0, is_preprint=0,
                            refs=[], refs_source=None
                        )
                        cache.upsert_work(pw)

                    seen.add(rk)
                    to_expand.append((rk, next_depth))
                    cache.set_frontier(seed_key, rk, next_depth)

                # Count it (as a shoulder) if not excluded
                rw = cache.get_work(rk)
                if rw:
                    if rw.is_review:
                        excluded_reviews += 1
                        continue
                    if rw.is_preprint:
                        excluded_preprints += 1
                        continue
                    counted.add(rk)
                    if rw.year:
                        earliest_year = rw.year if earliest_year is None else min(earliest_year, rw.year)

                if len(counted) >= args.max_nodes:
                    print(f"Reached max-nodes={args.max_nodes}; stopping traversal.")
                    to_expand = []
                    break

        if args.sleep:
            time.sleep(args.sleep)

    # Report
    print("\n=== RESULT ===")
    print(f"Seed DOI: {doi}")
    print(f"Unique shoulders counted (non-review, non-preprint): {len(counted)}")
    print(f"Excluded reviews (observed): {excluded_reviews}")
    print(f"Excluded preprints (observed): {excluded_preprints}")
    print(f"Terminal works (no references in OA and no S2 fallback): {terminal_no_refs}")
    print(f"Expansions using OpenAlex refs: {used_oa_refs}")
    print(f"Expansions using S2 fallback refs: {used_s2_refs}")
    print(f"Max depth reached (<= {args.max_depth}): {max_depth_reached}")
    print(f"Earliest year reached (where known): {earliest_year}")

    print("\nCache DB:", args.db)
    print("Tip: rerun with the same DB to continue/refine without refetching.")


if __name__ == "__main__":
    main()

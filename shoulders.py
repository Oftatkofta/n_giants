#!/usr/bin/env python3
"""
shoulders.py — Count unique "shoulders" (backward citation closure) for a seed paper.

Changes vs earlier version
- Loads API keys from .env (python-dotenv)
- OpenAlex-first traversal; Semantic Scholar fallback only when OA has no refs
- Excludes reviews + preprints (do not count, do not expand)
- Includes books/book chapters (counts + expands when refs exist)
- Actively resolves DOI-only/S2-only nodes into OpenAlex IDs when possible (better book coverage + types)
- Outputs: headline count, coverage stats, work-type histogram, oldest N works
- Exports edges + nodes to CSV for downstream analysis

Usage
  pip install requests python-dotenv
  python shoulders.py --doi 10.1128/mbio.00022-22 --max-depth 20

Optional env vars (in .env)
  OPENALEX_API_KEY=...
  OPENALEX_MAILTO=you@domain
  SEMANTIC_SCHOLAR_API_KEY=...
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sqlite3
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import requests
from dotenv import load_dotenv

load_dotenv()  # load .env from current working directory

OPENALEX_BASE = "https://api.openalex.org"
S2_BASE = "https://api.semanticscholar.org/graph/v1"

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
    return x.strip().lower()


def chunks(seq: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


@dataclass
class Work:
    key: str
    doi: Optional[str]
    title: Optional[str]
    year: Optional[int]
    oa_id: Optional[str]
    s2_id: Optional[str]
    oa_type: Optional[str]       # OpenAlex type, e.g. article, book, book-chapter, review, preprint
    s2_pub_types: Optional[str]  # JSON list
    venue: Optional[str]
    is_review: int
    is_preprint: int
    refs: List[str]              # list of canonical keys (doi:/openalex:/s2:)
    refs_source: Optional[str]   # openalex|s2|year_cutoff|none


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
            CREATE TABLE IF NOT EXISTS edges (
              seed_key TEXT,
              src_key TEXT,
              dst_key TEXT,
              depth INTEGER,
              source TEXT,
              PRIMARY KEY(seed_key, src_key, dst_key)
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
            oa_type=row[6], s2_pub_types=row[7], venue=row[8],
            is_review=row[9], is_preprint=row[10],
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

    def add_edge(self, seed_key: str, src: str, dst: str, depth: int, source: str):
        self.conn.execute(
            "INSERT OR IGNORE INTO edges(seed_key,src_key,dst_key,depth,source) VALUES (?,?,?,?,?)",
            (seed_key, src, dst, depth, source),
        )
        self.conn.commit()

    def set_frontier(self, seed_key: str, key: str, depth: int):
        self.conn.execute(
            "INSERT OR IGNORE INTO frontier(seed_key,key,depth) VALUES (?,?,?)",
            (seed_key, key, depth),
        )
        self.conn.commit()

    def iter_edges(self, seed_key: str) -> Iterable[Tuple[str, str, int, str]]:
        for row in self.conn.execute(
            "SELECT src_key, dst_key, depth, source FROM edges WHERE seed_key=?",
            (seed_key,),
        ):
            yield row[0], row[1], row[2], row[3]

    def iter_works(self) -> Iterable[Work]:
        cur = self.conn.execute(
            "SELECT key,doi,title,year,oa_id,s2_id,oa_type,s2_pub_types,venue,is_review,is_preprint,refs_json,refs_source FROM works"
        )
        for row in cur:
            refs = json.loads(row[11]) if row[11] else []
            yield Work(
                key=row[0], doi=row[1], title=row[2], year=row[3], oa_id=row[4], s2_id=row[5],
                oa_type=row[6], s2_pub_types=row[7], venue=row[8],
                is_review=row[9], is_preprint=row[10], refs=refs, refs_source=row[12]
            )


class OpenAlexClient:
    def __init__(self):
        self.api_key = os.getenv("OPENALEX_API_KEY")
        self.mailto = os.getenv("OPENALEX_MAILTO")
        self.sess = requests.Session()

    def _params(self, extra: Dict[str, Any]) -> Dict[str, Any]:
        p = dict(extra)
        if self.api_key:
            p["api_key"] = self.api_key
        if self.mailto:
            p["mailto"] = self.mailto
        return p

    def resolve_doi(self, doi: str) -> Optional[Dict[str, Any]]:
        r = self.sess.get(
            f"{OPENALEX_BASE}/works",
            params=self._params({"filter": f"doi:{doi}", "per-page": 1}),
            timeout=30,
        )
        if r.status_code >= 400:
            print(r.text)
        r.raise_for_status()
        data = r.json()
        results = data.get("results") or []
        return results[0] if results else None

    def get_works_by_ids(self, oa_ids: List[str]) -> List[Dict[str, Any]]:
        out = []
        for oid in oa_ids:
            oid = oid.replace("https://openalex.org/", "")
            url = f"{OPENALEX_BASE}/works/W{oid[1:]}" if oid.startswith("W") else f"{OPENALEX_BASE}/works/{oid}"
            r = self.sess.get(
                url,
                params=self._params({"select": "id,doi,title,publication_year,type,host_venue,primary_location,referenced_works"}),
                timeout=30,
            )
            if r.status_code == 404:
                continue
            r.raise_for_status()
            out.append(r.json())
        return out



class SemanticScholarClient:
    def __init__(self):
        self.api_key = os.getenv("SEMANTIC_SCHOLAR_API_KEY")
        self.sess = requests.Session()
        if self.api_key:
            self.sess.headers.update({"x-api-key": self.api_key})

    def get_paper_by_doi(self, doi: str) -> Optional[Dict[str, Any]]:
        fields = "paperId,externalIds,title,year,venue,publicationTypes"
        r = self.sess.get(f"{S2_BASE}/paper/DOI:{doi}", params={"fields": fields}, timeout=30)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()

    def get_references(self, paper_id: str) -> Optional[List[Dict[str, Any]]]:
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
    is_review = 1 if (oa_type == "review") else 0
    is_preprint = 1 if (oa_type == "preprint") else 0

    if s2_pub_types:
        lowered = {x.lower() for x in s2_pub_types if isinstance(x, str)}
        if "review" in lowered:
            is_review = 1
        if "preprint" in lowered:
            is_preprint = 1

    if not is_review and title and REVIEW_TITLE_PAT.search(title):
        is_review = 1

    if not is_preprint and venue and PREPRINT_VENUE_PAT.search(venue):
        is_preprint = 1

    return is_review, is_preprint


def canonical_key(doi: Optional[str], oa_id: Optional[str], s2_id: Optional[str]) -> str:
    if doi:
        return f"doi:{norm_doi(doi)}"
    if oa_id:
        return f"openalex:{oa_id}"
    if s2_id:
        return f"s2:{s2_id}"
    return "unknown:" + str(hash((doi, oa_id, s2_id)))


def oa_extract_venue(rec: Dict[str, Any]) -> Optional[str]:
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


def export_csv(cache: Cache, seed_key: str, out_prefix: str):
    nodes_path = f"{out_prefix}_nodes.csv"
    edges_path = f"{out_prefix}_edges.csv"

    # Nodes
    with open(nodes_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["key", "doi", "title", "year", "oa_id", "s2_id", "oa_type", "venue", "is_review", "is_preprint", "refs_source"])
        for node in cache.iter_works():
            w.writerow([
                node.key, node.doi or "", node.title or "", node.year or "",
                node.oa_id or "", node.s2_id or "", node.oa_type or "", node.venue or "",
                node.is_review, node.is_preprint, node.refs_source or ""
            ])

    # Edges (restricted to the seed graph)
    with open(edges_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["seed_key", "src_key", "dst_key", "depth", "source"])
        for src, dst, depth, source in cache.iter_edges(seed_key):
            w.writerow([seed_key, src, dst, depth, source])

    print(f"\nExported:\n- {nodes_path}\n- {edges_path}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--doi", required=True)
    ap.add_argument("--db", default="shoulders_cache.sqlite")
    ap.add_argument("--max-depth", type=int, default=20)
    ap.add_argument("--min-year", type=int, default=0, help="Stop expanding works older than this year (0 disables)")
    ap.add_argument("--max-nodes", type=int, default=200000)
    ap.add_argument("--sleep", type=float, default=0.0)
    ap.add_argument("--oldest", type=int, default=50, help="How many oldest works to list")
    ap.add_argument("--export-prefix", default="shoulders", help="CSV export prefix")
    args = ap.parse_args()

    doi = norm_doi(args.doi)

    if not os.getenv("OPENALEX_API_KEY"):
        print("WARNING: OPENALEX_API_KEY not set. You may hit stricter limits.", file=sys.stderr)

    cache = Cache(args.db)
    oa = OpenAlexClient()
    s2 = SemanticScholarClient()

    # Resolve seed via OpenAlex first
    seed_rec = oa.resolve_doi(doi)
    if not seed_rec:
        print(f"Seed DOI not found in OpenAlex: {doi}", file=sys.stderr)
        sys.exit(2)

    seed_oa_id = seed_rec.get("id")
    seed_doi = seed_rec.get("doi") or doi
    seed_title = seed_rec.get("title")
    seed_year = seed_rec.get("publication_year")
    seed_type = seed_rec.get("type")
    seed_venue = oa_extract_venue(seed_rec)

    seed_is_review, seed_is_preprint = classify_review_preprint(seed_title, seed_type, None, seed_venue)
    if seed_is_review or seed_is_preprint:
        print("Seed classified as review or preprint; per your rules it would be excluded.", file=sys.stderr)
        sys.exit(2)

    seed_key = canonical_key(seed_doi, seed_oa_id, None)
    cache.upsert_work(Work(
        key=seed_key, doi=seed_doi, title=seed_title, year=int(seed_year) if seed_year else None,
        oa_id=seed_oa_id, s2_id=None, oa_type=seed_type, s2_pub_types=None, venue=seed_venue,
        is_review=0, is_preprint=0, refs=[], refs_source=None
    ))

    # Traversal state
    to_expand: List[Tuple[str, int]] = [(seed_key, 0)]
    seen: Set[str] = {seed_key}
    counted: Set[str] = set()  # shoulders (excluding seed)
    excluded_reviews = 0
    excluded_preprints = 0
    terminal_no_refs = 0
    used_oa_refs = 0
    used_s2_refs = 0
    max_depth_reached = 0

    type_hist: Dict[str, int] = {}
    oldest: List[Tuple[int, str, str]] = []  # (year, key, title)

    def note_count(node: Work):
        """Count node if eligible; update hist/oldest."""
        if node.key == seed_key:
            return
        if node.is_review:
            return
        if node.is_preprint:
            return
        counted.add(node.key)

        t = node.oa_type or "unknown"
        type_hist[t] = type_hist.get(t, 0) + 1

        if node.year:
            title = node.title or ""
            oldest.append((node.year, node.key, title))

    def ensure_openalex_for_key(k: str) -> Optional[Work]:
        """If a work is DOI-keyed and lacks OA ID, resolve via OpenAlex and update cache."""
        w = cache.get_work(k)
        if not w:
            return None
        if w.oa_id:
            return w
        if w.doi and k.startswith("doi:"):
            rec = oa.resolve_doi(norm_doi(w.doi))
            if rec:
                oa_id = rec.get("id")
                oa_doi = rec.get("doi") or w.doi
                title = rec.get("title") or w.title
                year = rec.get("publication_year") or w.year
                oa_type = rec.get("type") or w.oa_type
                venue = oa_extract_venue(rec) or w.venue

                is_rev, is_pre = classify_review_preprint(title, oa_type, None, venue)
                w.oa_id = oa_id
                w.doi = oa_doi
                w.title = title
                w.year = int(year) if year else w.year
                w.oa_type = oa_type
                w.venue = venue
                w.is_review = is_rev
                w.is_preprint = is_pre
                cache.upsert_work(w)
        return w

    while to_expand:
        batch: List[Tuple[str, int]] = []
        while to_expand and len(batch) < 50:
            batch.append(to_expand.pop(0))
        max_depth_reached = max(max_depth_reached, max(d for _, d in batch))

        # Resolve DOI-only keys to OpenAlex IDs for better expansion (books included if OA has them)
        for k, _d in batch:
            ensure_openalex_for_key(k)

        # Collect OA IDs needing fetch for expansion
        oa_ids_need: List[str] = []
        key_by_oa: Dict[str, str] = {}
        for k, _d in batch:
            w = cache.get_work(k)
            if not w:
                continue
            if w.refs_source is not None:
                continue
            if w.oa_id:
                oa_ids_need.append(w.oa_id)
                key_by_oa[w.oa_id] = k

        oa_recs = oa.get_works_by_ids(oa_ids_need) if oa_ids_need else []
        oa_rec_by_id = {r.get("id"): r for r in oa_recs if r.get("id")}

        for k, depth in batch:
            if depth >= args.max_depth:
                continue

            w = cache.get_work(k)
            if not w:
                continue

            # Re-check after OA resolution
            w = ensure_openalex_for_key(k) or w

            # Exclusions: do not expand
            if w.is_review:
                excluded_reviews += 1
                cache.upsert_work(Work(**{**w.__dict__, "refs": [], "refs_source": "excluded_review"}))
                continue
            if w.is_preprint:
                excluded_preprints += 1
                cache.upsert_work(Work(**{**w.__dict__, "refs": [], "refs_source": "excluded_preprint"}))
                continue

            # Count it
            note_count(w)

            # Year cutoff for expansion
            if args.min_year and w.year and w.year < args.min_year:
                cache.upsert_work(Work(**{**w.__dict__, "refs": [], "refs_source": "year_cutoff"}))
                continue

            refs: List[str] = []
            source_used: Optional[str] = None

            # OpenAlex refs
            if w.oa_id and w.oa_id in oa_rec_by_id:
                rec = oa_rec_by_id[w.oa_id]
                # refresh metadata from OA record
                w.title = rec.get("title") or w.title
                w.year = int(rec.get("publication_year")) if rec.get("publication_year") else w.year
                w.doi = rec.get("doi") or w.doi
                w.oa_type = rec.get("type") or w.oa_type
                w.venue = oa_extract_venue(rec) or w.venue
                w.is_review, w.is_preprint = classify_review_preprint(w.title, w.oa_type, None, w.venue)

                if w.is_review:
                    excluded_reviews += 1
                    cache.upsert_work(Work(**{**w.__dict__, "refs": [], "refs_source": "excluded_review"}))
                    continue
                if w.is_preprint:
                    excluded_preprints += 1
                    cache.upsert_work(Work(**{**w.__dict__, "refs": [], "refs_source": "excluded_preprint"}))
                    continue

                ref_oa_ids = rec.get("referenced_works") or []
                if ref_oa_ids:
                    used_oa_refs += 1
                    source_used = "openalex"
                    for rid in ref_oa_ids:
                        # OA referenced_works contains OA IDs (URLs)
                        if isinstance(rid, str):
                            rid = rid.replace("https://openalex.org/", "")
                            refs.append(f"openalex:{rid}")

            # Semantic Scholar fallback (only when OA had none)
            if source_used is None:
                s2_id = w.s2_id
                if not s2_id and w.doi:
                    sp = s2.get_paper_by_doi(norm_doi(w.doi))
                    if sp:
                        s2_id = sp.get("paperId")
                        w.s2_id = s2_id
                        pub_types = sp.get("publicationTypes") or None
                        w.s2_pub_types = json.dumps(pub_types) if pub_types else w.s2_pub_types
                        w.is_review, w.is_preprint = classify_review_preprint(w.title, w.oa_type, pub_types, w.venue)
                        cache.upsert_work(w)

                if s2_id:
                    s2_refs = s2.get_references(s2_id)
                    if s2_refs:
                        used_s2_refs += 1
                        source_used = "s2"
                        for cp in s2_refs:
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
                            refs.append(ck)

                            # cache minimal node
                            if not cache.get_work(ck):
                                cache.upsert_work(Work(
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
                                    refs_source=None
                                ))

            if source_used is None:
                terminal_no_refs += 1
                cache.upsert_work(Work(**{**w.__dict__, "refs": [], "refs_source": "none"}))
                continue

            # Persist refs and edges
            w.refs = refs
            w.refs_source = source_used
            cache.upsert_work(w)

            next_depth = depth + 1
            for rk in refs:
                # record edge for export
                cache.add_edge(seed_key, k, rk, next_depth, source_used)

                # Normalize OpenAlex referenced IDs into placeholders to fetch later
                if rk.startswith("openalex:"):
                    oa_id = rk.replace("openalex:", "", 1).replace("https://openalex.org/", "")
                    placeholder_key = f"openalex:{oa_id}"
                    if placeholder_key not in seen:
                        seen.add(placeholder_key)
                        cache.upsert_work(Work(
                            key=placeholder_key, doi=None, title=None, year=None,
                            oa_id=oa_id, s2_id=None, oa_type=None, s2_pub_types=None, venue=None,
                            is_review=0, is_preprint=0, refs=[], refs_source=None
                        ))
                        to_expand.append((placeholder_key, next_depth))
                        cache.set_frontier(seed_key, placeholder_key, next_depth)
                    continue

                # DOI/S2 keys
                if rk not in seen:
                    seen.add(rk)
                    if not cache.get_work(rk):
                        cache.upsert_work(Work(
                            key=rk, doi=None, title=None, year=None,
                            oa_id=None, s2_id=None, oa_type=None, s2_pub_types=None, venue=None,
                            is_review=0, is_preprint=0, refs=[], refs_source=None
                        ))
                    to_expand.append((rk, next_depth))
                    cache.set_frontier(seed_key, rk, next_depth)

                # Safety stop
                if len(counted) >= args.max_nodes:
                    to_expand = []
                    break

        if args.sleep:
            time.sleep(args.sleep)

    # Oldest list
    oldest_sorted = sorted((x for x in oldest if x[0] is not None), key=lambda t: t[0])[: args.oldest]

    print("\n=== RESULT ===")
    print(f"Seed DOI: {doi}")
    print(f"Unique shoulders counted (non-review, non-preprint): {len(counted)}")
    print(f"Excluded reviews (observed): {excluded_reviews}")
    print(f"Excluded preprints (observed): {excluded_preprints}")
    print(f"Terminal works (no refs in OA; no S2 fallback): {terminal_no_refs}")
    print(f"Expansions using OpenAlex refs: {used_oa_refs}")
    print(f"Expansions using S2 fallback refs: {used_s2_refs}")
    print(f"Max depth reached (<= {args.max_depth}): {max_depth_reached}")

    print("\nWork-type histogram (OpenAlex types; unknown if not resolved):")
    for t, n in sorted(type_hist.items(), key=lambda kv: (-kv[1], kv[0])):
        print(f"  {t}: {n}")

    if oldest_sorted:
        print(f"\nOldest {len(oldest_sorted)} works reached:")
        for y, k, title in oldest_sorted:
            print(f"  {y}  {k}  {title[:120]}")

    export_csv(cache, seed_key, args.export_prefix)
    print("\nCache DB:", args.db)


if __name__ == "__main__":
    main()

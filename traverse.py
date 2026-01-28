from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Set, List, Dict, Any, Tuple
from core import WorkNode, canonical_key, classify_review_preprint, normalize_openalex_id
from store import Store
import time
from collections import deque
import requests

@dataclass
class Metrics:
    missing_oa_works: int = 0
    counted: int = 0
    excluded_reviews: int = 0
    excluded_preprints: int = 0
    terminal_no_refs: int = 0
    expanded_openalex: int = 0
    expanded_s2: int = 0
    cache_hit: int = 0
    cache_miss: int = 0


class Traverser:
    def __init__(self, store: Store, oa, s2=None, max_depth: int = 20, min_year: int = 0):
        self.store = store
        self.oa = oa
        self.s2 = s2
        self.max_depth = max_depth
        self.min_year = min_year

    def _upsert_from_oa(self, node: WorkNode, rec: dict[str, Any]) -> WorkNode:
        node.oa_id = normalize_openalex_id(rec["id"])
        node.doi = rec.get("doi") or node.doi
        node.title = rec.get("title") or node.title
        node.year = rec.get("publication_year") or node.year
        node.oa_type = rec.get("type") or node.oa_type

        # Best-effort venue
        hv = rec.get("host_venue") or {}
        node.venue = (hv.get("display_name") if isinstance(hv, dict) else None) or node.venue

        node.is_review, node.is_preprint = classify_review_preprint(node.title, node.oa_type, None, node.venue)
        self.store.upsert(node)
        return node

    def expand_one(self, key: str, depth: int, metrics: Metrics) -> List[Tuple[str, int]]:
        if depth >= self.max_depth:
            return []

        node = self.store.get(key) or WorkNode(key=key)

        # Cache hit:
        # - openalex-missing: known missing -> don't retry
        # - openalex: only trust if refs is non-empty
        if node.refs is not None and node.refs_source in ("openalex-missing", "openalex-empty"):
            metrics.cache_hit += 1
            return []

        if node.refs_source == "openalex" and node.refs:

            metrics.cache_hit += 1
            return [(rk, depth + 1) for rk in node.refs]


        # Infer OpenAlex ID from key if placeholder
        if node.oa_id is None and key.startswith("openalex:"):
            node.oa_id = key.split(":", 1)[1]
            self.store.upsert(node)

        # Do not expand excluded nodes
        if node.is_review:
            metrics.excluded_reviews += 1
            return []
        if node.is_preprint:
            metrics.excluded_preprints += 1
            return []

        # Year cutoff
        if self.min_year and node.year and node.year < self.min_year:
            return []

        # Prefer OpenAlex if we have oa_id, otherwise try resolving from DOI
        rec = None
        if node.oa_id:
            metrics.cache_miss += 1
            try:
                rec = self.oa.get_work(node.oa_id)
            except requests.HTTPError as e:
                if e.response is not None and e.response.status_code in (404, 410):
                    rec = None
                else:
                    raise

        # If we don't have a work via oa_id, try DOI resolution (for doi:* keys)
        if not rec and node.doi and key.startswith("doi:"):
            metrics.cache_miss += 1
            try:
                r = self.oa.resolve_doi(node.doi)
            except requests.HTTPError as e:
                if e.response is not None and e.response.status_code in (404, 410):
                    r = None
                else:
                    raise
            if r:
                metrics.cache_miss += 1
                rec = self.oa.get_work(r["id"])

        missing_by_id = bool(node.oa_id and not rec)
        # Still nothing -> treat as missing/terminal and move on
        if not rec:
            if missing_by_id:
                metrics.missing_oa_works += 1
            metrics.terminal_no_refs += 1

            # cache the failure so we don't retry forever
            node.refs = []
            node.refs_source = "openalex-missing"
            self.store.upsert(node)
            return []

        refs: list[str] = []

        metrics.expanded_openalex += 1
        node = self._upsert_from_oa(node, rec)
        for rid in rec.get("referenced_works") or []:
            refs.append(f"openalex:{normalize_openalex_id(rid)}")

        if len(refs) == 0:
            node.refs = []
            node.refs_source = "openalex-empty"
            self.store.upsert(node)
            metrics.terminal_no_refs += 1
            return []

        node.refs = refs
        node.refs_source = "openalex"
        self.store.upsert(node)
        # Persist refs + edges and schedule next frontier
        next_items: list[Tuple[str, int]] = []
        for rkey in refs:
            self.store.add_edge(key, rkey, depth + 1, "openalex")
            if not self.store.get(rkey):
                self.store.upsert(WorkNode(key=rkey))
            next_items.append((rkey, depth + 1))

        # counting: count referenced nodes when they are later enriched; keep simple here
        return next_items

    def run(self, seed_key: str) -> Metrics:
        
        metrics = Metrics()
        seen: Set[str] = {seed_key}
        queue = deque([(seed_key, 0)])

        processed = 0
        last_print = time.time()
        max_depth_seen = 0
        max_seen = 5_000_000
        new_since_print = 0

        while queue:
            key, depth = queue.popleft()
            
            if depth > max_depth_seen:
                max_depth_seen = depth
            
            processed += 1

            if len(seen) >= max_seen:
                print(f"[STOP] reached max_seen={max_seen}", flush=True)
                queue.clear()
                break

            if processed % 100 == 0 or (time.time() - last_print) > 5:
                now = time.time()
                dt = now - last_print
                rate = (new_since_print / dt) if dt > 0 else 0.0
                hit = metrics.cache_hit
                miss = metrics.cache_miss
                util = (hit / (hit + miss) * 100.0) if (hit + miss) else 0.0


                print(
                    f"[depth cur={depth:2d} max={max_depth_seen:2d}] "
                    f"processed={processed} "
                    f"queue={len(queue)} seen={len(seen)} "
                    f"expanded_oa={metrics.expanded_openalex} "
                    f"terminal={metrics.terminal_no_refs} "
                    f"missing_oa={metrics.missing_oa_works}",
                    f"new/sec={rate:6.1f}",
                    f"cache={util:5.1f}% (hit={hit} miss={miss}) ",
                    flush=True,
                )

                last_print = now
                new_since_print = 0

            try:
                for nxt_key, nxt_depth in self.expand_one(key, depth, metrics):
                    if nxt_key in seen:
                        continue
                    seen.add(nxt_key)
                    new_since_print += 1
                    queue.append((nxt_key, nxt_depth))
            
            except KeyboardInterrupt:
                raise
            except requests.HTTPError as e:
                if e.response is not None and e.response.status_code in (404, 410):
                    continue
                print(f"[ERROR] key={key} depth={depth} err={e}", flush=True)
            except Exception as e:
                print(f"[ERROR] key={key} depth={depth} err={e}", flush=True)


        # Count eligible nodes (excluding seed) based on reached keys
        for k in seen:
            if k == seed_key:
                continue
            node = self.store.get(k)
            if node and (node.is_review or node.is_preprint):
                continue
            metrics.counted += 1

        return metrics

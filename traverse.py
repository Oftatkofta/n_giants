from __future__ import annotations

import logging
import random
import time
from collections import Counter, deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import requests

from core import WorkNode, canonical_key, classify_review_preprint, normalize_openalex_id
from helpers import open_paths_writer, emit_terminal
from store import Store

logger = logging.getLogger(__name__)
# logger.info("traverse module loaded")


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

    def ensure_refs_cached(
        self, key: str, metrics: Metrics
    ) -> tuple[list[str], Literal["openalex", "openalex-empty", "openalex-missing"]]:
        node = self.store.get(key) or WorkNode(key=key)

        # Cached (including terminals)
        if node.refs is not None and node.refs_source in ("openalex", "openalex-empty", "openalex-missing"):
            metrics.cache_hit += 1
            # If cached record indicates a terminal, reflect that in metrics on reruns.
            if node.refs_source in ("openalex-empty", "openalex-missing"):
                metrics.terminal_no_refs += 1
                if node.refs_source == "openalex-missing":
                    metrics.missing_oa_works += 1
            return list(node.refs), node.refs_source  # type: ignore[return-value]

        # Infer OA id from key if needed
        if node.oa_id is None and key.startswith("openalex:"):
            node.oa_id = key.split(":", 1)[1]
            self.store.upsert(node)

        # Exclusions / cutoff -> terminal empty (and cached)
        if node.is_review:
            metrics.excluded_reviews += 1
            node.refs, node.refs_source = [], "openalex-empty"
            self.store.upsert(node)
            return [], "openalex-empty"

        if node.is_preprint:
            metrics.excluded_preprints += 1
            node.refs, node.refs_source = [], "openalex-empty"
            self.store.upsert(node)
            return [], "openalex-empty"

        if self.min_year and node.year and node.year < self.min_year:
            node.refs, node.refs_source = [], "openalex-empty"
            self.store.upsert(node)
            return [], "openalex-empty"

        # Fetch from OA
        rec: dict[str, Any] | None = None
        if node.oa_id:
            metrics.cache_miss += 1
            rec = self.oa.get_work(node.oa_id)

        # Optional DOI resolve path if you ever use doi:* keys here
        if not rec and node.doi and key.startswith("doi:"):
            metrics.cache_miss += 1
            r = self.oa.resolve_doi(node.doi)
            if r:
                metrics.cache_miss += 1
                rec = self.oa.get_work(r["id"])

        if not rec:
            metrics.missing_oa_works += 1
            metrics.terminal_no_refs += 1
            node.refs, node.refs_source = [], "openalex-missing"
            self.store.upsert(node)
            return [], "openalex-missing"

        metrics.expanded_openalex += 1
        node = self._upsert_from_oa(node, rec)

        refs: list[str] = [f"openalex:{normalize_openalex_id(rid)}" for rid in (rec.get("referenced_works") or [])]
        if not refs:
            metrics.terminal_no_refs += 1
            node.refs, node.refs_source = [], "openalex-empty"
            self.store.upsert(node)
            return [], "openalex-empty"

        node.refs, node.refs_source = refs, "openalex"
        self.store.upsert(node)
        return refs, "openalex"

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

    def expand_one(self, key: str, depth: int, metrics: Metrics) -> list[tuple[str, int]]:
        if depth >= self.max_depth:
            return []
        refs, src = self.ensure_refs_cached(key, metrics)
        if not refs:
            return []
        next_items: list[tuple[str, int]] = []
        for rkey in refs:
            self.store.add_edge(key, rkey, depth + 1, src)
            if not self.store.get(rkey):
                self.store.upsert(WorkNode(key=rkey))
            next_items.append((rkey, depth + 1))
        return next_items

    def random_walk_once(self, seed_key: str, rng: random.Random, max_steps: int, metrics: Metrics) -> tuple[int, str]:
        cur = seed_key
        depth = 0
        for _ in range(max_steps):
            refs, src = self.ensure_refs_cached(cur, metrics)
            if not refs:
                return depth, src
            cur = rng.choice(refs)
            depth += 1
        return depth, "max_steps"

    def random_walks(
        self,
        seed_key: str,
        n: int,
        seed: int = 0,
        max_steps: int = 100,
        heartbeat_sec: float = 5.0,
        heartbeat_every: int = 1000,
    ):
        rng = random.Random(seed)
        metrics = Metrics()
        depths: list[int] = []
        reasons = Counter()
        t0 = time.time()
        last_print = t0
        last_i = 0

        for i in range(1, n + 1):
            d, reason = self.random_walk_once(seed_key, rng, max_steps, metrics)
            depths.append(d)
            reasons[reason] += 1
            now = time.time()
            if (i % heartbeat_every == 0) or (now - last_print >= heartbeat_sec):
                dt = now - last_print
                rate = (i - last_i) / dt if dt > 0 else 0.0
                hit, miss = metrics.cache_hit, metrics.cache_miss
                util = (hit / (hit + miss) * 100.0) if (hit + miss) else 0.0
                logger.info(
                    f"[walk {i:>7}/{n}] "
                    f"rate={rate:6.1f}/s "
                    f"cache={util:5.1f}% "
                    f"term={dict(reasons)}"
                )
                last_print = now
                last_i = i

        # summary stats
        depths.sort()

        def q(p: float) -> int:
            if not depths:
                return 0
            i = int(round((p / 100.0) * (len(depths) - 1)))
            return depths[max(0, min(i, len(depths) - 1))]

        hit, miss = metrics.cache_hit, metrics.cache_miss
        util = (hit / (hit + miss) * 100.0) if (hit + miss) else 0.0

        return {
            "n": n,
            "p50": q(50),
            "p90": q(90),
            "p99": q(99),
            "max": depths[-1] if depths else 0,
            "reasons": dict(reasons),
            "cache_util_pct": util,
            "metrics": metrics,
            "elapsed_sec": time.time() - t0,
        }

    def dfs(
        self,
        seed_key: str,
        dfs_order: str = "as-listed",
        dfs_limit: int = 100_000,
        stop_on_terminal: bool = False,
        stop_on_missing: bool = False,
        record_paths: str = "",
        rng_seed: int = 0,
    ) -> Metrics:
        """
        Depth-first traversal over references using ensure_refs_cached().

        Options:
          - dfs_order: 'as-listed' | 'year' (oldest-first) | 'random' (seeded)
          - dfs_limit: max visited nodes
          - stop_on_terminal: stop after first terminal node
          - stop_on_missing: if a node is 'openalex-missing', treat as terminal and optionally stop
          - record_paths: JSONL path; each terminal produces {"reason":..., "depth":..., "path":[...]}
        """
        metrics = Metrics()
        rng = random.Random(rng_seed)

        # stack holds (key, depth, path_list)
        stack: list[tuple[str, int, list[str]]] = [(seed_key, 0, [seed_key])]
        seen: set[str] = set()
        visited = 0

        # Use shared writer helpers (behavior-neutral)
        with open_paths_writer(record_paths) as writer:
            def _emit(reason: str, depth: int, path: list[str]) -> None:
                emit_terminal(writer, reason, depth, path)

            while stack:
                key, depth, path = stack.pop()
                if key in seen:
                    continue
                seen.add(key)
                visited += 1
                if visited >= dfs_limit:
                    break

                if depth >= self.max_depth:
                    _emit("max_depth", depth, path)
                    if stop_on_terminal:
                        break
                    continue

                refs, src = self.ensure_refs_cached(key, metrics)

                # missing handling
                if src == "openalex-missing":
                    _emit("openalex-missing", depth, path)
                    if stop_on_terminal or stop_on_missing:
                        break
                    continue

                # terminal handling
                if not refs:
                    # src will typically be openalex-empty (or missing handled above)
                    _emit(src, depth, path)
                    if stop_on_terminal:
                        break
                    continue

                # order refs according to option
                ordered = list(refs)
                if dfs_order == "random":
                    rng.shuffle(ordered)
                elif dfs_order == "year":
                    # oldest-first (smaller year first). Unknown years go last.
                    def y(k: str) -> int:
                        n = self.store.get(k)
                        return (n.year if (n and n.year) else 9999)
                    ordered.sort(key=y)

                # DFS: push in reverse so first in 'ordered' is processed next
                for rk in reversed(ordered):
                    if rk not in seen:
                        stack.append((rk, depth + 1, path + [rk]))

        # Count eligible nodes (excluding seed) based on reached keys
        for k in seen:
            if k == seed_key:
                continue
            node = self.store.get(k)
            if node and (node.is_review or node.is_preprint):
                continue
            metrics.counted += 1

        return metrics

    def dfs_promote_longest(
        self,
        seed_key: str,
        rng_seed: int = 0,
        dfs_limit: int = 100_000,
        record_paths: str = "",
    ) -> Metrics:
        """
        Hybrid traversal mode: random descent to terminal, then one-level backtrack
        and oldest-first sibling exploration with path promotion.

        Algorithm:
        1) RANDOM descent from the seed until a terminal is reached.
            -> This initial terminal path is main_path; emit terminal JSONL if requested.
        2) Backtrack EXACTLY ONE LEVEL (parent of the terminal).
        3) Explore parent's remaining children OLDEST-FIRST (by year).
            For each sibling:
            - Perform a RANDOM descent to terminal (emit JSONL).
            - If that candidate path is STRICTLY LONGER than main_path:
                    main_path = candidate; repeat from step (2) with the new parent.
        4) Stop when no sibling improves the main path or dfs_limit/max_depth are reached.

        Returns:
        Metrics gathered during the run. Terminal accounting uses ensure_refs_cached(),
        so cached terminals on reruns are counted correctly.
        """
        rng = random.Random(rng_seed)
        metrics = Metrics()

        # --- helpers ---

        def year_of(k: str) -> int:
            n = self.store.get(k)
            return n.year if (n and n.year) else 9999

        def terminal_descent(start_key: str) -> tuple[list[str], str]:
            """
            Randomly descend from start_key until reaching a terminal (or max depth).
            Returns (path, reason).
            """
            key = start_key
            depth = 0
            path: list[str] = [key]
            while True:
                refs, src = self.ensure_refs_cached(key, metrics)
                if not refs or depth >= self.max_depth:
                    return path, src
                key = rng.choice(refs)
                depth += 1
                path.append(key)

        # Use the shared JSONL writer/emitter (same contract as dfs())
        from helpers import open_paths_writer, emit_terminal  # already imported at module top in your refactor

        with open_paths_writer(record_paths) as writer:
            def _emit(reason: str, depth: int, path: list[str]) -> None:
                emit_terminal(writer, reason, depth, path)

            # 1) initial random descent
            main_path, main_reason = terminal_descent(seed_key)
            _emit(main_reason, len(main_path) - 1, main_path)

            visited_nodes = len(main_path)
            if visited_nodes >= dfs_limit:
                # Count eligible nodes on the best path found so far
                for k in set(main_path):
                    if k == seed_key:
                        continue
                    node = self.store.get(k)
                    if node and (node.is_review or node.is_preprint):
                        continue
                    metrics.counted += 1
                return metrics

            # 2) one-level backtrack loop with promotion
            while True:
                if len(main_path) < 2:
                    break  # cannot backtrack if only the seed is present

                parent = main_path[-2]
                refs, _ = self.ensure_refs_cached(parent, metrics)

                # siblings: other children of parent (exclude the one used by main_path)
                used_child = main_path[-1]
                siblings = [r for r in refs if r != used_child]
                siblings.sort(key=year_of)  # oldest-first by year

                improved = False
                for sib in siblings:
                    if visited_nodes >= dfs_limit:
                        break

                    cand_path, cand_reason = terminal_descent(sib)
                    visited_nodes += len(cand_path)

                    _emit(cand_reason, len(cand_path) - 1, cand_path)

                    # strictly longer path is promoted
                    if len(cand_path) > len(main_path):
                        main_path = cand_path
                        main_reason = cand_reason
                        improved = True
                        break  # restart loop from new main_path's parent

                if not improved:
                    break

        # Final metrics: count eligible nodes on the best path
        for k in set(main_path):
            if k == seed_key:
                continue
            node = self.store.get(k)
            if node and (node.is_review or node.is_preprint):
                continue
            metrics.counted += 1

        return metrics

    def run(self, seed_key: str) -> Metrics:
        metrics = Metrics()
        seen: set[str] = {seed_key}
        queue = deque([(seed_key, 0)])
        processed = 0
        last_print = time.time()
        max_depth_seen = 0
        max_seen = 50_000_000
        new_since_print = 0

        while queue:
            key, depth = queue.popleft()
            if depth > max_depth_seen:
                max_depth_seen = depth
            processed += 1

            if len(seen) >= max_seen:
                logger.info(f"[STOP] reached max_seen={max_seen}")
                queue.clear()
                break

            if processed % 10000 == 0 or (time.time() - last_print) > 5:
                now = time.time()
                dt = now - last_print
                rate = (new_since_print / dt) if dt > 0 else 0.0
                hit = metrics.cache_hit
                miss = metrics.cache_miss
                util = (hit / (hit + miss) * 100.0) if (hit + miss) else 0.0
                logger.info(
                    f"depth={depth:2d} "
                    f"processed={processed} "
                    f"queue={len(queue)} seen={len(seen)} "
                    f"expanded_oa={metrics.expanded_openalex} "
                    f"terminal={metrics.terminal_no_refs} "
                    f"missing_oa={metrics.missing_oa_works} "
                    f"new/sec={rate:6.1f} "
                    f"cache={util:5.1f}% (hit={hit} miss={miss})"
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
                    # treat as missing work; don't spam stderr
                    metrics.missing_oa_works += 1
                    metrics.terminal_no_refs += 1
                    node = self.store.get(key) or WorkNode(key=key)
                    node.refs = []
                    node.refs_source = "openalex-missing"
                    self.store.upsert(node)
                    continue
                logger.error(f"[ERROR] key={key} depth={depth} err={e}")
            except Exception as e:
                logger.error(f"[ERROR] key={key} depth={depth} err={e}")

        # Count eligible nodes (excluding seed) based on reached keys
        for k in seen:
            if k == seed_key:
                continue
            node = self.store.get(k)
            if node and (node.is_review or node.is_preprint):
                continue
            metrics.counted += 1

        return metrics
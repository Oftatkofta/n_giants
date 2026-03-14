# run.py
from __future__ import annotations

import argparse
import logging
import os
import random

from dotenv import load_dotenv

from clients import OpenAlexClient
from core import WorkNode, normalize_openalex_id
from helpers import setup_logging, summarize_paths
from sqlite_store import SQLiteStore
from traverse import Traverser

# Initialize logging once (idempotent if helpers.setup_logging has a handler guard)
setup_logging("walk.log")


def main() -> None:
    load_dotenv()
    api_key = os.getenv("OPENALEX_API_KEY")
    mailto = os.getenv("OPENALEX_MAILTO")

    ap = argparse.ArgumentParser()
    ap.add_argument("--doi", required=True)
    ap.add_argument("--db", default="shoulders_cache.sqlite")
    ap.add_argument("--max-depth", type=int, default=20)
    ap.add_argument("--min-year", type=int, default=0)

    ap.add_argument(
        "--mode",
        choices=["bfs", "walk", "dfs", "promote-longest"],
        default="bfs",
        help=(
            "Traversal mode: "
            "bfs (full frontier), "
            "walk (random lineage sampling), "
            "dfs (depth-first search), "
            "promote-longest (random descent, one-level backtrack, oldest-first sibling exploration with path promotion)."
        ),
    )

    # Random-walk options
    ap.add_argument(
        "--walks", type=int, default=10000, help="Number of random walks (mode=walk)."
    )
    ap.add_argument(
        "--walk-seed",
        type=int,
        default=random.randint(0, 1_000_000),
        help="RNG seed for random walks and DFS ordering when applicable.",
    )
    ap.add_argument(
        "--walk-max-steps",
        type=int,
        default=5000,
        help="Max steps per walk before forcing termination (mode=walk).",
    )

    # DFS/Hybrid-specific
    ap.add_argument(
        "--dfs-order",
        choices=["as-listed", "year", "random"],
        default="as-listed",
        help="DFS child ordering: as-listed (OpenAlex order), year (oldest-first), random (seeded).",
    )
    ap.add_argument(
        "--dfs-limit",
        type=int,
        default=100,
        help="Max number of nodes to visit in DFS/hybrid (guardrail).",
    )
    ap.add_argument(
        "--stop-on-terminal",
        action="store_true",
        help="Stop DFS after reaching the first terminal node (one lineage).",
    )
    ap.add_argument(
        "--stop-on-missing",
        action="store_true",
        help="Treat openalex-missing as terminal and stop (useful for clean paths).",
    )
    ap.add_argument(
        "--record-paths",
        default="",
        help="If set, write visited terminal paths as JSONL to this file.",
    )
    ap.add_argument(
        "--paths-top-k",
        type=int,
        default=10,
        help="How many paths to show in the end-of-run summary (when --record-paths is set).",
    )
    ap.add_argument(
        "--paths-show",
        choices=["both", "deepest", "oldest", "none"],
        default="both",
        help="Which path summaries to print from --record-paths.",
    )

    # Batch/parallel fetching options
    ap.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Number of works to prefetch in parallel (BFS mode). Set to 0 to disable batching.",
    )
    ap.add_argument(
        "--concurrency",
        type=int,
        default=30,
        help="Max concurrent API requests (requires aiohttp).",
    )

    args = ap.parse_args()

    if not api_key:
        raise SystemExit("Missing OPENALEX_API_KEY in environment/.env")

    oa = OpenAlexClient(api_key=api_key, mailto=mailto, concurrency=args.concurrency)
    store = SQLiteStore(args.db)

    # Resolve seed DOI -> OpenAlex Work
    doi = args.doi
    rec = oa.resolve_doi(doi)
    if not rec:
        raise SystemExit(f"Could not resolve DOI in OpenAlex: {doi}")
    seed_work = oa.get_work(rec["id"])
    seed_oa_id = normalize_openalex_id(seed_work["id"])
    seed_key = f"openalex:{seed_oa_id}"

    # Ensure seed exists in store
    if not store.get(seed_key):
        store.upsert(WorkNode(key=seed_key, oa_id=seed_oa_id))

    # Traverser (BFS/DFS/Hybrid/Walk)
    t = Traverser(
        store,
        oa=oa,
        max_depth=args.max_depth,
        min_year=args.min_year,
        batch_size=args.batch_size,
    )

    if args.mode == "bfs":
        use_batch = args.batch_size > 0
        metrics = t.run(seed_key, use_batch=use_batch)
        logging.info("=== RUN COMPLETE (BFS) ===")
        logging.info("Seed DOI: %s", args.doi)
        logging.info("Seed key: %s", seed_key)
        logging.info("Metrics: %s", metrics)

    elif args.mode == "dfs":
        metrics = t.dfs(
            seed_key=seed_key,
            dfs_order=args.dfs_order,
            dfs_limit=args.dfs_limit,
            stop_on_terminal=args.stop_on_terminal,
            stop_on_missing=args.stop_on_missing,
            record_paths=args.record_paths,
            rng_seed=args.walk_seed,  # reuse walk seed for reproducibility
        )
        logging.info("=== RUN COMPLETE (DFS) ===")
        logging.info("Seed DOI: %s", args.doi)
        logging.info("Seed key: %s", seed_key)
        logging.info("DFS order: %s limit=%s", args.dfs_order, args.dfs_limit)
        if args.record_paths:
            logging.info("Recorded terminal paths to: %s", args.record_paths)
        logging.info("Metrics: %s", metrics)
        if args.record_paths and args.paths_show != "none":
            summarize_paths(args.db, args.record_paths, top_k=args.paths_top_k, show=args.paths_show)

    elif args.mode == "promote-longest":
        metrics = t.dfs_promote_longest(
            seed_key=seed_key,
            rng_seed=args.walk_seed,     # seed the random descent
            dfs_limit=args.dfs_limit,
            record_paths=args.record_paths,
        )
        logging.info("=== RUN COMPLETE (PROMOTE-LONGEST) ===")
        logging.info("Seed DOI: %s", args.doi)
        logging.info("Seed key: %s", seed_key)
        if args.record_paths:
            logging.info("Recorded terminal paths to: %s", args.record_paths)
            if args.paths_show != "none":
                summarize_paths(args.db, args.record_paths, top_k=args.paths_top_k, show=args.paths_show)
        logging.info("Metrics: %s", metrics)

    else:  # mode == "walk"
        out = t.random_walks(
            seed_key=seed_key,
            n=args.walks,
            seed=args.walk_seed,
            max_steps=args.walk_max_steps,
        )
        logging.info("=== RUN COMPLETE (RANDOM WALKS) ===")
        logging.info("Seed DOI: %s", args.doi)
        logging.info("Seed key: %s", seed_key)
        logging.info("Walks: %s", out["n"])
        logging.info(
            "Depth p50/p90/p99/max: %s %s %s %s",
            out["p50"], out["p90"], out["p99"], out["max"]
        )
        logging.info("Termination reasons: %s", out["reasons"])
        logging.info("Cache util: %.1f%%", out["cache_util_pct"])
        logging.info("Metrics: %s", out["metrics"])


if __name__ == "__main__":
    main()

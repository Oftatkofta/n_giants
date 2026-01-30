# run.py
from __future__ import annotations

import os
import argparse
from dotenv import load_dotenv

from clients import OpenAlexClient
from sqlite_store import SQLiteStore
from traverse import Traverser
from core import WorkNode, normalize_openalex_id
import logging

import logging
logging.basicConfig(
    filename="walk.log",
    level=logging.INFO,
    format="%(asctime)s %(message)s",
)

def main():
    load_dotenv()

    api_key = os.getenv("OPENALEX_API_KEY")
    mailto = os.getenv("OPENALEX_MAILTO")

    ap = argparse.ArgumentParser()
    ap.add_argument("--doi", required=True)
    ap.add_argument("--db", default="shoulders_cache.sqlite")
    ap.add_argument("--max-depth", type=int, default=20)
    ap.add_argument("--min-year", type=int, default=0)

    ap.add_argument("--mode", choices=["bfs", "walk"], default="bfs",
                help="Traversal mode: bfs (full frontier) or walk (random lineage sampling).")
    ap.add_argument("--walks", type=int, default=10000,
                    help="Number of random walks (mode=walk).")
    ap.add_argument("--walk-seed", type=int, default=0,
                    help="RNG seed for random walks (mode=walk).")
    ap.add_argument("--walk-max-steps", type=int, default=5000,
                    help="Max steps per walk before forcing termination (mode=walk).")

    args = ap.parse_args()

    if not api_key:
        raise SystemExit("Missing OPENALEX_API_KEY in environment/.env")

    oa = OpenAlexClient(api_key=api_key, mailto=mailto)
    store = SQLiteStore(args.db)

    doi = args.doi

    # Resolve seed DOI -> OpenAlex Work
    rec = oa.resolve_doi(doi)
    if not rec:
        raise SystemExit(f"Could not resolve DOI in OpenAlex: {doi}")

    seed_work = oa.get_work(rec["id"])
    seed_oa_id = normalize_openalex_id(seed_work["id"])
    seed_key = f"openalex:{seed_oa_id}"

    # Ensure seed exists in store
    if not store.get(seed_key):
        store.upsert(WorkNode(key=seed_key, oa_id=seed_oa_id))

    # Traverse
    t = Traverser(
        store,
        oa=oa,
        max_depth=args.max_depth,
        min_year=args.min_year,
    )

    if args.mode == "bfs":
        metrics = t.run(seed_key)
        print("=== RUN COMPLETE (BFS) ===")
        print("Seed DOI:", args.doi)
        print("Seed key:", seed_key)
        print(metrics)

    else:
        out = t.random_walks(
            seed_key=seed_key,
            n=args.walks,
            seed=args.walk_seed,
            max_steps=args.walk_max_steps,
        )

        print("=== RUN COMPLETE (RANDOM WALKS) ===")
        print("Seed DOI:", args.doi)
        print("Seed key:", seed_key)
        print("Walks:", out["n"])
        print("Depth p50 / p90 / p99 / max:",
            out["p50"], out["p90"], out["p99"], out["max"])
        print("Termination reasons:", out["reasons"])
        print(f"Cache util: {out['cache_util_pct']:.1f}%")
        print("Metrics:", out["metrics"])



if __name__ == "__main__":
    main()

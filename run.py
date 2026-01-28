# run.py
from __future__ import annotations

import os
import argparse
from dotenv import load_dotenv

from clients import OpenAlexClient
from sqlite_store import SQLiteStore
from traverse import Traverser
from core import WorkNode, normalize_openalex_id

def main():
    load_dotenv()

    api_key = os.getenv("OPENALEX_API_KEY")
    mailto = os.getenv("OPENALEX_MAILTO")

    ap = argparse.ArgumentParser()
    ap.add_argument("--doi", required=True)
    ap.add_argument("--db", default="shoulders_cache.sqlite")
    ap.add_argument("--max-depth", type=int, default=20)
    ap.add_argument("--min-year", type=int, default=0)
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
    t = Traverser(store=store, oa=oa, s2=None, max_depth=args.max_depth, min_year=args.min_year)
    metrics = t.run(seed_key)

    print("=== RUN COMPLETE ===")
    print("Seed DOI:", doi)
    print("Seed key:", seed_key)
    print(metrics)

if __name__ == "__main__":
    main()

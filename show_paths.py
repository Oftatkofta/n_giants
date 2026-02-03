#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from collections.abc import Iterable
from pathlib import Path


def fetch_works_chunked(
    conn: sqlite3.Connection,
    keys: list[str],
    chunk_size: int = 2000,
) -> dict[str, tuple[int | None, str | None, str | None]]:
    """
    Returns key -> (year, oa_type, title), fetching in chunks to avoid extremely long
    SQL statements on huge key sets. Keeps exact behavior for normal sizes.
    """
    out: dict[str, tuple[int | None, str | None, str | None]] = {}
    if not keys:
        return out

    for i in range(0, len(keys), chunk_size):
        batch = keys[i : i + chunk_size]
        qmarks = ",".join(["?"] * len(batch))
        rows = conn.execute(
            f"SELECT key, year, oa_type, title FROM works WHERE key IN ({qmarks})",
            batch,
        ).fetchall()
        for k, y, typ, title in rows:
            out[k] = (y, typ, title)
    return out


def load_records(jsonl_path: Path) -> list[dict[str, object]]:
    """
    Read JSONL lines -> list of records (only those that contain a list-like 'path').
    """
    records: list[dict[str, object]] = []
    with jsonl_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            path = rec.get("path") or []
            if not isinstance(path, list):
                continue
            records.append(rec)
    return records


def collect_unique_keys(records: Iterable[dict[str, object]]) -> list[str]:
    all_keys: list[str] = []
    for rec in records:
        path = rec.get("path") or []
        if not isinstance(path, list):
            continue
        for k in path:
            if isinstance(k, str):
                all_keys.append(k)
    return sorted(set(all_keys))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="shoulders_cache.sqlite", help="SQLite cache file")
    ap.add_argument("--jsonl", required=True, help="dfs_paths.jsonl file")
    ap.add_argument("--limit", type=int, default=20, help="Max number of paths to print")
    ap.add_argument(
        "--min-year",
        type=int,
        default=0,
        help="Only show paths whose terminal year <= min-year (0 disables)",
    )
    ap.add_argument(
        "--contains",
        default="",
        help="Only show paths where any title contains this substring (case-insensitive)",
    )
    ap.add_argument("--csv", default="", help="Optional CSV output path")

    # NEW: selection mode for printing
    ap.add_argument(
        "--print-mode",
        choices=["all", "longest", "oldest"],
        default="all",
        help=(
            "Which paths to print (capped by --limit): "
            "all = as they appear in the JSONL; "
            "longest = deepest paths first; "
            "oldest = oldest terminal-year first."
        ),
    )

    args = ap.parse_args()

    db_path = Path(args.db)
    jsonl_path = Path(args.jsonl)

    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")
    if not jsonl_path.exists():
        raise SystemExit(f"JSONL not found: {jsonl_path}")

    # Read JSONL
    records = load_records(jsonl_path)  # structure aligns with original behavior  [1](https://uppsalauniversitet-my.sharepoint.com/personal/jens_eriksson_imbim_uu_se/Documents/Microsoft%20Copilot%20Chat%20Files/show_paths.py)

    # Bulk fetch metadata for all unique keys
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row  # harmless, helps debugging
    uniq_keys = collect_unique_keys(records)
    meta = fetch_works_chunked(conn, uniq_keys)

    def title_of(k: str) -> str:
        y, typ, title = meta.get(k, (None, None, None))
        return title or "<missing title>"

    def year_of(k: str) -> int | None:
        y, _, _ = meta.get(k, (None, None, None))
        return y

    def format_node(k: str) -> str:
        y, typ, title = meta.get(k, (None, None, None))
        y_s = str(y) if y is not None else "????"
        typ_s = typ or "?"
        title_s = (title or "<missing title>").replace("\\n", " ").strip()
        return f"{k} ({y_s}, {typ_s}) — {title_s}"

    # Optional filters (same as before)
    contains = args.contains.strip().lower()

    def depth_of(rec: dict[str, object]) -> int:
        path = rec.get("path") or []
        return int(rec.get("depth", len(path) - 1)) if isinstance(path, list) else 0

    def term_year_of(rec: dict[str, object]) -> int:
        path = rec.get("path") or []
        if not isinstance(path, list) or not path:
            return 9999
        terminal_key = path[-1]
        y = year_of(terminal_key)
        return y if isinstance(y, int) else 9999

    # Apply per-record filters first (min-year, contains)
    filtered: list[dict[str, object]] = []
    for rec in records:
        path = rec.get("path") or []
        if not isinstance(path, list) or not path:
            continue

        # min-year filter
        terminal_key = path[-1]
        terminal_year = year_of(terminal_key)
        if args.min_year and (terminal_year is None or terminal_year > args.min_year):
            continue

        # contains filter
        if contains and not any(contains in title_of(k).lower() for k in path if isinstance(k, str)):
            continue

        filtered.append(rec)

    # Selection by print mode (then cap by --limit)
    if args.print_mode == "longest":
        selected = sorted(filtered, key=depth_of, reverse=True)[: args.limit]
    elif args.print_mode == "oldest":
        selected = sorted(filtered, key=term_year_of)[: args.limit]
    else:
        selected = filtered[: args.limit]

    # Print + collect CSV (same structure as before)
    out_rows: list[dict[str, object]] = []
    for idx, rec in enumerate(selected, start=1):
        path = rec.get("path") or []
        if not isinstance(path, list) or not path:
            continue

        reason = rec.get("reason", "")
        depth = rec.get("depth", len(path) - 1)

        # header
        print(f"\n=== Path {idx} reason={reason} depth={depth} ===")
        # body
        for i, k in enumerate(path):
            if not isinstance(k, str):
                continue
            print(f"{i:>2d}. {format_node(k)}")

        # CSV rows
        for i, k in enumerate(path):
            if not isinstance(k, str):
                continue
            y, typ, title = meta.get(k, (None, None, None))
            out_rows.append(
                {
                    "path_index": idx,
                    "step": i,
                    "key": k,
                    "year": y,
                    "type": typ,
                    "title": title,
                    "reason": reason if i == len(path) - 1 else "",
                    "depth": depth if i == len(path) - 1 else "",
                }
            )

    if args.csv:
        csv_path = Path(args.csv)
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(
                f,
                fieldnames=["path_index", "step", "key", "year", "type", "title", "reason", "depth"],
            )
            w.writeheader()
            w.writerows(out_rows)
        print(f"\nWrote CSV: {csv_path}")

    conn.close()


if __name__ == "__main__":
    main()
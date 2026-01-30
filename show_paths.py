import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def fetch_works(conn: sqlite3.Connection, keys: List[str]) -> Dict[str, Tuple[Optional[int], Optional[str], Optional[str]]]:
    """
    Returns key -> (year, oa_type, title)
    """
    if not keys:
        return {}
    qmarks = ",".join(["?"] * len(keys))
    rows = conn.execute(
        f"SELECT key, year, oa_type, title FROM works WHERE key IN ({qmarks})",
        keys,
    ).fetchall()
    out = {}
    for k, y, t, title in rows:
        out[k] = (y, t, title)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="shoulders_cache.sqlite", help="SQLite cache file")
    ap.add_argument("--jsonl", required=True, help="dfs_paths.jsonl file")
    ap.add_argument("--limit", type=int, default=20, help="Max number of paths to print")
    ap.add_argument("--min-year", type=int, default=0, help="Only show paths whose terminal year <= min-year (0 disables)")
    ap.add_argument("--contains", default="", help="Only show paths where any title contains this substring (case-insensitive)")
    ap.add_argument("--csv", default="", help="Optional CSV output path")
    args = ap.parse_args()

    db_path = Path(args.db)
    jsonl_path = Path(args.jsonl)

    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")
    if not jsonl_path.exists():
        raise SystemExit(f"JSONL not found: {jsonl_path}")

    conn = sqlite3.connect(str(db_path))

    # read JSONL lines
    records: List[Dict[str, Any]] = []
    all_keys: List[str] = []

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
            for k in path:
                if isinstance(k, str):
                    all_keys.append(k)

    # bulk fetch metadata
    meta = fetch_works(conn, sorted(set(all_keys)))

    def title_of(k: str) -> str:
        y, typ, title = meta.get(k, (None, None, None))
        return title or "<missing title>"

    def year_of(k: str) -> Optional[int]:
        y, _, _ = meta.get(k, (None, None, None))
        return y

    def format_node(k: str) -> str:
        y, typ, title = meta.get(k, (None, None, None))
        y_s = str(y) if y is not None else "????"
        typ_s = typ or "?"
        title_s = (title or "<missing title>").replace("\n", " ").strip()
        return f"{k} ({y_s}, {typ_s}) — {title_s}"

    # optional filters
    contains = args.contains.strip().lower()
    out_rows = []

    printed = 0
    for rec in records:
        path = rec.get("path") or []
        if not path:
            continue

        terminal_key = path[-1]
        terminal_year = year_of(terminal_key)

        if args.min_year and (terminal_year is None or terminal_year > args.min_year):
            continue

        if contains:
            if not any(contains in title_of(k).lower() for k in path if isinstance(k, str)):
                continue

        reason = rec.get("reason", "")
        depth = rec.get("depth", len(path) - 1)

        # print
        print(f"\n=== Path {printed+1} | reason={reason} depth={depth} ===")
        for i, k in enumerate(path):
            if not isinstance(k, str):
                continue
            print(f"{i:>2d}. {format_node(k)}")

        # collect for csv
        for i, k in enumerate(path):
            if not isinstance(k, str):
                continue
            y, typ, title = meta.get(k, (None, None, None))
            out_rows.append({
                "path_index": printed + 1,
                "step": i,
                "key": k,
                "year": y,
                "type": typ,
                "title": title,
                "reason": reason if i == len(path) - 1 else "",
                "depth": depth if i == len(path) - 1 else "",
            })

        printed += 1
        if printed >= args.limit:
            break

    if args.csv:
        import csv
        csv_path = Path(args.csv)
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["path_index","step","key","year","type","title","reason","depth"])
            w.writeheader()
            w.writerows(out_rows)
        print(f"\nWrote CSV: {csv_path}")

    conn.close()


if __name__ == "__main__":
    main()

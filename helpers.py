from __future__ import annotations

import json
import logging
import sqlite3
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def setup_logging(log_name: str = "walk.log", level: int = logging.INFO) -> Path:
    """
    Configure process-wide logging to both console and file.
    Must be called once from the entrypoint (run.py) before other modules log.
    """
    log_path = Path(__file__).with_name(log_name)

    # Prevent duplicate handler attachment
    if logging.getLogger().handlers:
        return log_path


    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )

    logging.info("Logging initialized → %s", log_path.resolve())
    return log_path

def format_path(path: List[str], meta: Dict[str, Tuple[Optional[int], Optional[str], Optional[str]]]) -> str:
    """
    Return a multi-line, human-readable path:
      i. key (year, type) — title
    """
    lines: List[str] = []
    for i, k in enumerate(path):
        y, typ, title = meta.get(k, (None, None, None))
        y_s = str(y) if y is not None else "????"
        typ_s = typ or "?"
        title_s = (title or "<missing title>").replace("\n", " ").strip()
        if len(title_s) > 140:
            title_s = title_s[:137] + "..."
        lines.append(f"{i:>2d}. {k} ({y_s}, {typ_s}) — {title_s}")
    return "\n".join(lines)



def summarize_paths(db_path: str, jsonl_path: str, top_k: int = 10, show: str = "both") -> None:

    """
    Read a DFS JSONL paths file, enrich with cached titles/years/types from SQLite,
    and log a compact report (reasons, deepest paths, oldest terminal-year paths).
    """
    p = Path(jsonl_path)
    if not p.exists():
        logging.info("No paths file found: %s", jsonl_path)
        return

    recs: List[Dict[str, Any]] = []
    all_keys: List[str] = []

    with p.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            r = json.loads(line)
            path = r.get("path") or []
            if not isinstance(path, list) or not path:
                continue
            recs.append(r)
            all_keys.extend([k for k in path if isinstance(k, str)])

    if not recs:
        logging.info("No valid paths in: %s", jsonl_path)
        return

    # Bulk fetch metadata
    conn = sqlite3.connect(db_path)
    uniq = sorted(set(all_keys))
    meta: Dict[str, Tuple[Optional[int], Optional[str], Optional[str]]] = {}

    if uniq:
        qmarks = ",".join(["?"] * len(uniq))
        rows = conn.execute(
            f"SELECT key, year, oa_type, title FROM works WHERE key IN ({qmarks})",
            uniq,
        ).fetchall()
        for k, y, typ, title in rows:
            meta[k] = (y, typ, title)
    conn.close()



    def term_year(r: Dict[str, Any]) -> int:
        k = r["path"][-1]
        y = meta.get(k, (None, None, None))[0]
        return y if isinstance(y, int) else 9999

    def depth_of(r: Dict[str, Any]) -> int:
        return int(r.get("depth", len(r["path"]) - 1))

    reasons = Counter(r.get("reason", "unknown") for r in recs)

    logging.info("=== DFS PATH SUMMARY ===")
    logging.info("Recorded paths: %d", len(recs))
    logging.info("Termination reasons: %s", dict(reasons))

    if show in ("both", "deepest"):
        deepest = sorted(recs, key=depth_of, reverse=True)[:top_k]
        logging.info("--- Top %d deepest terminal paths ---", top_k)
        for i, r in enumerate(deepest, 1):
            d = depth_of(r)
            y = term_year(r)
            reason = r.get("reason", "unknown")
            logging.info("#%d depth=%d terminal_year=%s reason=%s", i, d, ("????" if y == 9999 else y), reason)
            logging.info("\n%s", format_path(r["path"], meta))

    if show in ("both", "oldest"):
        oldest = sorted(recs, key=term_year)[:top_k]
        logging.info("--- Top %d oldest terminal-year paths ---", top_k)
        for i, r in enumerate(oldest, 1):
            d = depth_of(r)
            y = term_year(r)
            reason = r.get("reason", "unknown")
            logging.info("#%d terminal_year=%s depth=%d reason=%s", i, ("????" if y == 9999 else y), d, reason)
            logging.info("\n%s", format_path(r["path"], meta))




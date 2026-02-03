from __future__ import annotations

import json
import logging
import sqlite3
from collections import Counter
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import TextIO


def setup_logging(log_name: str = "walk.log", level: int = logging.INFO) -> Path:
    """
    Configure process-wide logging to both console and file.
    Must be called once from the entrypoint (run.py) before other modules log.

    Notes:
        - Idempotent: if handlers are already attached, this returns immediately.
        - File is written next to this module: <repo>/walk.log by default.
    """
    log_path = Path(__file__).with_name(log_name)

    # Prevent duplicate handler attachment across repeated runs/tests
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


def format_path(
    path: list[str],
    meta: dict[str, tuple[int | None, str | None, str | None]],
) -> str:
    """
    Return a multi-line, human-readable path:

        i. key (year, type) — title

    Args:
        path: The sequence of keys from seed to terminal.
        meta: Mapping key -> (year, oa_type, title) from the SQLite cache.

    Returns:
        A pretty-printed string showing each step with cached metadata.
    """
    lines: list[str] = []
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
    Read a DFS/Hybrid JSONL paths file, enrich with cached titles/years/types from SQLite,
    and log a compact report (reasons, deepest paths, oldest terminal-year paths).

    Args:
        db_path: Path to the SQLite cache.
        jsonl_path: Path to the JSONL file containing terminal paths.
        top_k: How many paths to display per category.
        show: "both" | "deepest" | "oldest" | "none".
    """
    p = Path(jsonl_path)
    if not p.exists():
        logging.info("No paths file found: %s", jsonl_path)
        return

    recs: list[dict[str, object]] = []
    all_keys: list[str] = []

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
    meta: dict[str, tuple[int | None, str | None, str | None]] = {}
    if uniq:
        qmarks = ",".join(["?"] * len(uniq))
        rows = conn.execute(
            f"SELECT key, year, oa_type, title FROM works WHERE key IN ({qmarks})",
            uniq,
        ).fetchall()
        for k, y, typ, title in rows:
            meta[k] = (y, typ, title)
    conn.close()

    def term_year(r: dict[str, object]) -> int:
        k = r["path"][-1]  # type: ignore[index]
        y = meta.get(k, (None, None, None))[0]
        return y if isinstance(y, int) else 9999

    def depth_of(r: dict[str, object]) -> int:
        # default depth = len(path)-1 (seed at depth 0)
        return int(r.get("depth", len(r["path"]) - 1))  # type: ignore[arg-type, index]

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
            logging.info(
                "#%d depth=%d terminal_year=%s reason=%s",
                i, d, ("????" if y == 9999 else y), reason,
            )
            logging.info("\n%s", format_path(r["path"], meta))  # type: ignore[arg-type]

    if show in ("both", "oldest"):
        oldest = sorted(recs, key=term_year)[:top_k]
        logging.info("--- Top %d oldest terminal-year paths ---", top_k)
        for i, r in enumerate(oldest, 1):
            d = depth_of(r)
            y = term_year(r)
            reason = r.get("reason", "unknown")
            logging.info(
                "#%d terminal_year=%s depth=%d reason=%s",
                i, ("????" if y == 9999 else y), d, reason,
            )
            logging.info("\n%s", format_path(r["path"], meta))  # type: ignore[arg-type]


# --- Path recording helpers (shared by dfs / hybrid / others) ---

@contextmanager
def open_paths_writer(record_paths: str) -> Iterator[TextIO | None]:
    """
    Open a JSONL path output file if 'record_paths' is given, otherwise yield None.

    Behavior:
      • If record_paths is empty/falsy → yield None and do nothing.
      • If record_paths is provided:
            - ensure parent directories exist
            - open the file in append mode (UTF‑8)
            - yield the file handle
            - automatically close it afterward (even on exceptions)

    Matches dfs()/dfs_promote_longest() writer lifecycle.
    """
    if not record_paths:
        yield None
        return

    p = Path(record_paths)
    p.parent.mkdir(parents=True, exist_ok=True)
    f = p.open("a", encoding="utf-8")
    try:
        yield f
    finally:
        f.close()


def emit_terminal(writer: TextIO | None, reason: str, depth: int, path: list[str]) -> None:
    """
    Emit a terminal JSONL record (same format used by dfs):

        {
          "reason": "<terminal-type>",
          "depth": <int>,
          "path": ["k1", "k2", ...]
        }

    If writer is None, this is a no-op (consistent with dfs behavior).
    """
    if writer is None:
        return

    writer.write(json.dumps({"reason": reason, "depth": depth, "path": path}) + "\n")
    writer.flush()
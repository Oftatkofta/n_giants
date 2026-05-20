from __future__ import annotations

import json
import logging
import os
import pickle
import time
from typing import Any, Optional

logger = logging.getLogger(__name__)


class BfsCheckpoint:
    """
    BFS resume state kept OUTSIDE the main cache database.

    - seen journal: append-only, one key per line (cheap during traversal)
    - queue + metadata: written only on interrupt or explicit save (not periodic)
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.seen_path = f"{db_path}.bfs.seen.journal"
        self.queue_path = f"{db_path}.bfs.queue.pkl"
        self.meta_path = f"{db_path}.bfs.meta.json"
        self._seen_file: Optional[Any] = None

    @staticmethod
    def run_id(seed_key: str, max_depth: int, min_year: int) -> str:
        return f"{seed_key}|d{max_depth}|y{min_year}"

    def has_checkpoint(self, seed_key: str, max_depth: int, min_year: int) -> bool:
        if not os.path.exists(self.meta_path):
            return False
        try:
            with open(self.meta_path, encoding="utf-8") as f:
                meta = json.load(f)
        except (OSError, json.JSONDecodeError):
            return False
        return meta.get("run_id") == self.run_id(seed_key, max_depth, min_year)

    def init_fresh_run(self, seed_key: str) -> None:
        """Start a new run journal (not used when resuming)."""
        self.close_journal()
        with open(self.seen_path, "w", encoding="utf-8") as f:
            f.write(f"{seed_key}\n")

    def open_journal_append(self) -> None:
        self.close_journal()
        self._seen_file = open(self.seen_path, "a", encoding="utf-8", buffering=1024 * 1024)

    def journal_seen(self, key: str) -> None:
        if self._seen_file is None:
            return
        self._seen_file.write(f"{key}\n")

    def flush_journal(self) -> None:
        if self._seen_file is not None:
            self._seen_file.flush()

    def close_journal(self) -> None:
        if self._seen_file is not None:
            self._seen_file.flush()
            self._seen_file.close()
            self._seen_file = None

    def save(
        self,
        seed_key: str,
        max_depth: int,
        min_year: int,
        processed: int,
        queue: list[tuple[str, int]],
    ) -> None:
        self.close_journal()
        meta = {
            "run_id": self.run_id(seed_key, max_depth, min_year),
            "seed_key": seed_key,
            "max_depth": max_depth,
            "min_year": min_year,
            "processed": processed,
            "updated_at": int(time.time()),
            "queue_len": len(queue),
            "format": 1,
        }
        meta_tmp = f"{self.meta_path}.tmp"
        queue_tmp = f"{self.queue_path}.tmp"
        with open(meta_tmp, "w", encoding="utf-8") as f:
            json.dump(meta, f)
        with open(queue_tmp, "wb") as f:
            pickle.dump(queue, f, protocol=4)
        os.replace(meta_tmp, self.meta_path)
        os.replace(queue_tmp, self.queue_path)
        logger.info(
            "BFS checkpoint saved to sidecar files (queue=%s, seen journal=%s)",
            len(queue),
            self.seen_path,
        )

    def load(
        self,
        seed_key: str,
        max_depth: int,
        min_year: int,
        legacy_store: Any = None,
    ) -> Optional[dict[str, object]]:
        run_id = self.run_id(seed_key, max_depth, min_year)

        if os.path.exists(self.meta_path) and os.path.exists(self.queue_path):
            with open(self.meta_path, encoding="utf-8") as f:
                meta = json.load(f)
            if meta.get("run_id") == run_id:
                logger.info("Loading BFS checkpoint from sidecar files...")
                t0 = time.time()
                with open(self.queue_path, "rb") as f:
                    queue = pickle.load(f)
                seen: set[str] = set()
                if os.path.exists(self.seen_path):
                    with open(self.seen_path, encoding="utf-8") as f:
                        for line in f:
                            key = line.rstrip("\n")
                            if key:
                                seen.add(key)
                logger.info(
                    "Sidecar checkpoint loaded in %.1fs (queue=%s seen=%s)",
                    time.time() - t0,
                    len(queue),
                    len(seen),
                )
                return {
                    "seed_key": meta["seed_key"],
                    "max_depth": meta["max_depth"],
                    "min_year": meta["min_year"],
                    "processed": meta["processed"],
                    "updated_at": meta["updated_at"],
                    "queue": queue,
                    "seen": seen,
                }

        if legacy_store is not None and hasattr(legacy_store, "load_bfs_checkpoint"):
            logger.info("Trying legacy SQLite checkpoint tables...")
            return legacy_store.load_bfs_checkpoint(seed_key, max_depth, min_year)

        return None

    def clear_run(self, seed_key: str, max_depth: int, min_year: int, legacy_store: Any = None) -> None:
        self.close_journal()
        for path in (self.seen_path, self.queue_path, self.meta_path):
            try:
                os.remove(path)
            except FileNotFoundError:
                pass
        if legacy_store is not None and hasattr(legacy_store, "clear_bfs_checkpoint"):
            legacy_store.clear_bfs_checkpoint(seed_key, max_depth, min_year)

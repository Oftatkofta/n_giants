# sqlite_store.py
from __future__ import annotations

import json
import sqlite3
import time
from functools import lru_cache
from typing import Optional

from core import WorkNode


class SQLiteStore:
    """
    Drop-in Store implementation backed by SQLite.

    Stores:
      - works: node metadata + cached refs
      - edges: citation edges (optionally with depth + source)
    """

    def __init__(self, path: str = "shoulders_cache.sqlite", cache_size_mb: int = 128):
        self.path = path
        self.conn = sqlite3.connect(path)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self.conn.execute(f"PRAGMA cache_size = -{cache_size_mb * 1024};")  # Negative = KB
        self._init_schema()
        # In-memory LRU cache for hot items
        self._cache: dict[str, Optional[WorkNode]] = {}
        self._cache_max = 100_000  # Keep up to 100k items in memory

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

    def close(self):
        self.conn.close()

    def _init_schema(self):
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS works (
              key TEXT PRIMARY KEY,
              doi TEXT,
              oa_id TEXT,
              s2_id TEXT,
              title TEXT,
              year INTEGER,
              oa_type TEXT,
              venue TEXT,
              is_review INTEGER,
              is_preprint INTEGER,
              refs_json TEXT,
              refs_source TEXT,
              updated_at INTEGER
            );
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS edges (
              src_key TEXT,
              dst_key TEXT,
              depth INTEGER,
              source TEXT,
              PRIMARY KEY (src_key, dst_key, depth, source)
            );
            """
        )
        self.conn.commit()

    def get(self, key: str) -> Optional[WorkNode]:
        # Check in-memory cache first
        if key in self._cache:
            return self._cache[key]

        row = self.conn.execute(
            """
            SELECT key, doi, oa_id, s2_id, title, year, oa_type, venue,
                   is_review, is_preprint, refs_json, refs_source
            FROM works WHERE key=?
            """,
            (key,),
        ).fetchone()

        if not row:
            self._cache_put(key, None)
            return None

        refs = json.loads(row[10]) if row[10] else None

        node = WorkNode(
            key=row[0],
            doi=row[1],
            oa_id=row[2],
            s2_id=row[3],
            title=row[4],
            year=row[5],
            oa_type=row[6],
            venue=row[7],
            is_review=bool(row[8]),
            is_preprint=bool(row[9]),
            refs=refs,
            refs_source=row[11],
        )
        self._cache_put(key, node)
        return node

    def _cache_put(self, key: str, node: Optional[WorkNode]) -> None:
        """Add item to in-memory cache, evicting old items if needed."""
        if len(self._cache) >= self._cache_max:
            # Simple eviction: remove first 10% of items
            keys_to_remove = list(self._cache.keys())[: self._cache_max // 10]
            for k in keys_to_remove:
                del self._cache[k]
        self._cache[key] = node

    def upsert(self, node: WorkNode) -> None:
        self.conn.execute(
            """
            INSERT INTO works (
              key, doi, oa_id, s2_id, title, year, oa_type, venue,
              is_review, is_preprint, refs_json, refs_source, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
              doi=excluded.doi,
              oa_id=excluded.oa_id,
              s2_id=excluded.s2_id,
              title=excluded.title,
              year=excluded.year,
              oa_type=excluded.oa_type,
              venue=excluded.venue,
              is_review=excluded.is_review,
              is_preprint=excluded.is_preprint,
              refs_json=excluded.refs_json,
              refs_source=excluded.refs_source,
              updated_at=excluded.updated_at
            """,
            (
                node.key,
                node.doi,
                node.oa_id,
                node.s2_id,
                node.title,
                node.year,
                node.oa_type,
                node.venue,
                int(bool(node.is_review)),
                int(bool(node.is_preprint)),
                json.dumps(node.refs) if node.refs is not None else None,
                node.refs_source,
                int(time.time()),
            ),
        )
        self.conn.commit()
        # Update in-memory cache
        self._cache_put(node.key, node)

    def get_batch(self, keys: list[str]) -> dict[str, Optional[WorkNode]]:
        """
        Fetch multiple works in a single query.
        Returns a dict mapping key -> WorkNode (or None if not found).
        Much faster than calling get() repeatedly for large batches.
        """
        if not keys:
            return {}

        result: dict[str, Optional[WorkNode]] = {}

        # Check in-memory cache first
        uncached_keys = []
        for key in keys:
            if key in self._cache:
                result[key] = self._cache[key]
            else:
                uncached_keys.append(key)

        if not uncached_keys:
            return result

        # Batch query for uncached keys
        placeholders = ",".join("?" * len(uncached_keys))
        rows = self.conn.execute(
            f"""
            SELECT key, doi, oa_id, s2_id, title, year, oa_type, venue,
                   is_review, is_preprint, refs_json, refs_source
            FROM works WHERE key IN ({placeholders})
            """,
            uncached_keys,
        ).fetchall()

        # Build result from rows
        found_keys = set()
        for row in rows:
            refs = json.loads(row[10]) if row[10] else None
            node = WorkNode(
                key=row[0],
                doi=row[1],
                oa_id=row[2],
                s2_id=row[3],
                title=row[4],
                year=row[5],
                oa_type=row[6],
                venue=row[7],
                is_review=bool(row[8]),
                is_preprint=bool(row[9]),
                refs=refs,
                refs_source=row[11],
            )
            result[row[0]] = node
            self._cache_put(row[0], node)
            found_keys.add(row[0])

        # Mark missing keys as None
        for key in uncached_keys:
            if key not in found_keys:
                result[key] = None
                self._cache_put(key, None)

        return result

    def add_edge(self, src: str, dst: str, depth: int, source: str) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO edges(src_key, dst_key, depth, source) VALUES (?, ?, ?, ?)",
            (src, dst, depth, source),
        )
        self.conn.commit()

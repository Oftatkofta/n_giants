# sqlite_store.py
from __future__ import annotations

import json
import sqlite3
import time
from typing import Optional

from core import WorkNode


class SQLiteStore:
    """
    Drop-in Store implementation backed by SQLite.

    Stores:
      - works: node metadata + cached refs
      - edges: citation edges (optionally with depth + source)
    """

    def __init__(self, path: str = "shoulders_cache.sqlite"):
        self.path = path
        self.conn = sqlite3.connect(path)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self._init_schema()

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
        row = self.conn.execute(
            """
            SELECT key, doi, oa_id, s2_id, title, year, oa_type, venue,
                   is_review, is_preprint, refs_json, refs_source
            FROM works WHERE key=?
            """,
            (key,),
        ).fetchone()

        if not row:
            return None

        refs = json.loads(row[10]) if row[10] else None

        return WorkNode(
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

    def add_edge(self, src: str, dst: str, depth: int, source: str) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO edges(src_key, dst_key, depth, source) VALUES (?, ?, ?, ?)",
            (src, dst, depth, source),
        )
        self.conn.commit()

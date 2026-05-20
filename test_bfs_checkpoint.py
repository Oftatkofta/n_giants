import os
import tempfile

from bfs_checkpoint import BfsCheckpoint
from core import WorkNode
from sqlite_store import SQLiteStore
from traverse import Metrics, Traverser


class FakeOA:
    def __init__(self, by_id):
        self.by_id = by_id

    def get_work(self, oa_id):
        return self.by_id[oa_id]

    def get_works_batch(self, oa_ids):
        return {oa_id: self.by_id.get(oa_id) for oa_id in oa_ids}

    def resolve_doi(self, doi):
        return None


def test_bfs_checkpoint_sidecar_roundtrip():
    fd, path = tempfile.mkstemp(suffix=".sqlite")
    os.close(fd)
    sidecar_paths = [
        f"{path}.bfs.seen.journal",
        f"{path}.bfs.queue.pkl",
        f"{path}.bfs.meta.json",
    ]

    try:
        cp = BfsCheckpoint(path)
        cp.init_fresh_run("openalex:W1")
        cp.open_journal_append()
        cp.journal_seen("openalex:W2")
        cp.journal_seen("openalex:W3")
        cp.save(
            "openalex:W1",
            10,
            0,
            processed=2,
            queue=[("openalex:W3", 1), ("openalex:W4", 2)],
        )

        loaded = cp.load("openalex:W1", 10, 0)
        assert loaded is not None
        assert loaded["processed"] == 2
        assert loaded["queue"] == [("openalex:W3", 1), ("openalex:W4", 2)]
        assert loaded["seen"] == {"openalex:W1", "openalex:W2", "openalex:W3"}
    finally:
        os.remove(path)
        for sidecar in sidecar_paths:
            try:
                os.remove(sidecar)
            except FileNotFoundError:
                pass


def test_bfs_checkpoint_resume_traversal():
    fd, path = tempfile.mkstemp(suffix=".sqlite")
    os.close(fd)

    try:
        fake = FakeOA(
            {
                "W1": {
                    "id": "https://openalex.org/W1",
                    "title": "Seed",
                    "publication_year": 2022,
                    "type": "article",
                    "referenced_works": ["https://openalex.org/W2", "https://openalex.org/W3"],
                },
                "W2": {
                    "id": "https://openalex.org/W2",
                    "title": "B",
                    "publication_year": 2010,
                    "type": "article",
                    "referenced_works": ["https://openalex.org/W4"],
                },
                "W3": {
                    "id": "https://openalex.org/W3",
                    "title": "C",
                    "publication_year": 2011,
                    "type": "article",
                    "referenced_works": [],
                },
                "W4": {
                    "id": "https://openalex.org/W4",
                    "title": "D",
                    "publication_year": 1980,
                    "type": "book",
                    "referenced_works": [],
                },
            }
        )

        store = SQLiteStore(path, mmap_gb=0)
        store.upsert(WorkNode(key="openalex:W1", oa_id="W1"))
        t = Traverser(store, oa=fake, max_depth=10, batch_size=1)

        cp = BfsCheckpoint(path)
        cp.init_fresh_run("openalex:W1")
        cp.open_journal_append()
        cp.journal_seen("openalex:W1")
        cp.journal_seen("openalex:W2")
        cp.journal_seen("openalex:W3")
        cp.journal_seen("openalex:W4")
        cp.save(
            "openalex:W1",
            10,
            0,
            processed=2,
            queue=[("openalex:W3", 1), ("openalex:W4", 2)],
        )

        metrics = t.run("openalex:W1", use_batch=False, resume=True, checkpoint_interval_sec=0)
        assert metrics.counted == 3
        store.close()
    finally:
        os.remove(path)
        for suffix in (".bfs.seen.journal", ".bfs.queue.pkl", ".bfs.meta.json"):
            try:
                os.remove(path + suffix)
            except FileNotFoundError:
                pass


def test_warm_cache_skips_cached_keys():
    fd, path = tempfile.mkstemp(suffix=".sqlite")
    os.close(fd)

    try:
        store = SQLiteStore(path, mmap_gb=0)
        store.upsert(
            WorkNode(
                key="openalex:W1",
                oa_id="W1",
                refs=["openalex:W2"],
                refs_source="openalex",
            )
        )
        store.get("openalex:W1")
        assert "openalex:W1" in store._cache

        store.upsert(
            WorkNode(
                key="openalex:W2",
                oa_id="W2",
                refs=[],
                refs_source="openalex-empty",
            )
        )

        store.warm_cache(["openalex:W1", "openalex:W2"])
        assert store.get("openalex:W2") is not None
        store.close()
    finally:
        os.remove(path)


def test_expand_one_skips_existing_edges():
    fd, path = tempfile.mkstemp(suffix=".sqlite")
    os.close(fd)

    try:
        store = SQLiteStore(path, mmap_gb=0)
        store.upsert(
            WorkNode(
                key="openalex:W1",
                oa_id="W1",
                refs=["openalex:W2", "openalex:W3"],
                refs_source="openalex",
            )
        )
        store.add_edges(
            [
                ("openalex:W1", "openalex:W2", 1, "openalex"),
                ("openalex:W1", "openalex:W3", 1, "openalex"),
            ]
        )

        t = Traverser(store, oa=FakeOA({}), max_depth=10)
        m = Metrics()
        items = t.expand_one("openalex:W1", 0, m)
        assert items == [("openalex:W2", 1), ("openalex:W3", 1)]
        assert store.conn.execute("SELECT COUNT(*) FROM edges").fetchone()[0] == 2
        store.close()
    finally:
        os.remove(path)

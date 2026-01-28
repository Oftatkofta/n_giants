import os
import tempfile

from core import WorkNode
from sqlite_store import SQLiteStore


def test_sqlite_store_roundtrip():
    # create temp db file
    fd, path = tempfile.mkstemp(suffix=".sqlite")
    os.close(fd)

    try:
        store = SQLiteStore(path)

        # insert node
        n1 = WorkNode(
            key="openalex:W1",
            oa_id="W1",
            title="Test Paper",
            year=2020,
            oa_type="article",
            is_review=False,
            is_preprint=False,
        )
        store.upsert(n1)

        # fetch node
        got = store.get("openalex:W1")
        assert got is not None
        assert got.title == "Test Paper"
        assert got.year == 2020
        assert got.oa_id == "W1"

        # update node
        n1.title = "Updated Title"
        store.upsert(n1)
        got2 = store.get("openalex:W1")
        assert got2.title == "Updated Title"

        # insert edge
        store.add_edge("openalex:W1", "openalex:W2", depth=1, source="openalex")

        # reopen store to test persistence
        store.close()
        store2 = SQLiteStore(path)

        got3 = store2.get("openalex:W1")
        assert got3 is not None
        assert got3.title == "Updated Title"
        store2.close()


    finally:
        try:
            store.close()
        except Exception:
            pass
        try:
            store2.close()
        except Exception:
            pass
        os.remove(path)

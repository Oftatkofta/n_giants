import os
import tempfile

from core import WorkNode
from sqlite_store import SQLiteStore


def test_sqlite_store_refs_roundtrip():
    fd, path = tempfile.mkstemp(suffix=".sqlite")
    os.close(fd)

    try:
        with SQLiteStore(path) as store:
            n = WorkNode(
                key="openalex:W1",
                oa_id="W1",
                refs=["openalex:W2", "doi:10.1000/xyz", "s2:abc123"],
                refs_source="openalex",
            )
            store.upsert(n)

            got = store.get("openalex:W1")
            assert got is not None
            assert got.refs == ["openalex:W2", "doi:10.1000/xyz", "s2:abc123"]
            assert got.refs_source == "openalex"

            # update refs
            n.refs = ["openalex:W9"]
            n.refs_source = "s2"
            store.upsert(n)

            got2 = store.get("openalex:W1")
            assert got2.refs == ["openalex:W9"]
            assert got2.refs_source == "s2"

    finally:
        os.remove(path)

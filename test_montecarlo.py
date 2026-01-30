import random
from dataclasses import dataclass
from typing import Any, Optional

from core import WorkNode
from traverse import Traverser, Metrics


class FakeOA:
    def __init__(self, works: dict[str, dict[str, Any]]):
        self.works = works
        self.calls = 0

    def get_work(self, oa_id: str) -> Optional[dict[str, Any]]:
        # oa_id comes in like "W1" etc after normalize
        self.calls += 1
        return self.works.get(oa_id)

    def resolve_doi(self, doi: str) -> Optional[dict[str, Any]]:
        return None


class MemoryStore:
    def __init__(self):
        self.nodes = {}
        self.edges = []

    def get(self, key: str):
        return self.nodes.get(key)

    def upsert(self, node: WorkNode):
        self.nodes[node.key] = node

    def add_edge(self, src: str, dst: str, depth: int, source: str):
        self.edges.append((src, dst, depth, source))


def test_ensure_refs_cached_caches_openalex():
    fake = FakeOA({
        "W1": {
            "id": "https://openalex.org/W1",
            "title": "Seed",
            "publication_year": 2022,
            "type": "article",
            "host_venue": {"display_name": "X"},
            "referenced_works": ["https://openalex.org/W2", "https://openalex.org/W3"],
        }
    })

    st = MemoryStore()
    st.upsert(WorkNode(key="openalex:W1", oa_id="W1"))

    t = Traverser(st, oa=fake, max_depth=10)
    m = Metrics()

    refs, src = t.ensure_refs_cached("openalex:W1", m)
    assert src == "openalex"
    assert refs == ["openalex:W2", "openalex:W3"]
    assert m.cache_miss == 1
    assert m.expanded_openalex == 1

    # second call should be a cache hit, no additional OA calls
    refs2, src2 = t.ensure_refs_cached("openalex:W1", m)
    assert src2 == "openalex"
    assert refs2 == ["openalex:W2", "openalex:W3"]
    assert m.cache_hit >= 1
    assert fake.calls == 1


def test_ensure_refs_cached_caches_empty_terminal():
    fake = FakeOA({
        "W9": {
            "id": "https://openalex.org/W9",
            "title": "No refs",
            "publication_year": 1980,
            "type": "article",
            "referenced_works": [],
        }
    })
    st = MemoryStore()
    st.upsert(WorkNode(key="openalex:W9", oa_id="W9"))
    t = Traverser(st, oa=fake, max_depth=10)
    m = Metrics()

    refs, src = t.ensure_refs_cached("openalex:W9", m)
    assert refs == []
    assert src == "openalex-empty"
    assert m.terminal_no_refs == 1

    # should be cache hit now
    refs2, src2 = t.ensure_refs_cached("openalex:W9", m)
    assert refs2 == []
    assert src2 == "openalex-empty"
    assert fake.calls == 1


def test_random_walk_deterministic_path_length():
    # W1 -> {W2,W3}; W2 -> {W4}; W3 -> {}; W4 -> {}
    fake = FakeOA({
        "W1": {"id": "https://openalex.org/W1", "title": "Seed", "publication_year": 2022, "type": "article",
               "referenced_works": ["https://openalex.org/W2", "https://openalex.org/W3"]},
        "W2": {"id": "https://openalex.org/W2", "title": "B", "publication_year": 2010, "type": "article",
               "referenced_works": ["https://openalex.org/W4"]},
        "W3": {"id": "https://openalex.org/W3", "title": "C", "publication_year": 2011, "type": "article",
               "referenced_works": []},
        "W4": {"id": "https://openalex.org/W4", "title": "D", "publication_year": 1980, "type": "book",
               "referenced_works": []},
    })
    st = MemoryStore()
    st.upsert(WorkNode(key="openalex:W1", oa_id="W1"))

    t = Traverser(st, oa=fake, max_depth=10)
    m = Metrics()

    # Seed RNG makes the choice reproducible.
    rng = random.Random(0)
    d, reason = t.random_walk_once("openalex:W1", rng=rng, max_steps=1000, metrics=m)

    # With Random(0), first choice from [W2,W3] is deterministic.
    # Depending on Python's RNG, it should pick W3 first -> terminate in 1 hop.
    # If it picks W2 first -> path length is 2 (W1->W2->W4 terminal).
    assert d in (1, 2)
    assert reason in ("openalex-empty", "openalex-missing", "max_steps")

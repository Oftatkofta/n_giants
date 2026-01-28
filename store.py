from __future__ import annotations
from typing import Protocol, Optional, Iterable, Tuple
from core import WorkNode

class Store(Protocol):
    def get(self, key: str) -> Optional[WorkNode]: ...
    def upsert(self, node: WorkNode) -> None: ...
    def add_edge(self, src: str, dst: str, depth: int, source: str) -> None: ...

class MemoryStore:
    def __init__(self):
        self.nodes: dict[str, WorkNode] = {}
        self.edges: set[tuple[str, str, int, str]] = set()

    def get(self, key: str) -> Optional[WorkNode]:
        return self.nodes.get(key)

    def upsert(self, node: WorkNode) -> None:
        self.nodes[node.key] = node

    def add_edge(self, src: str, dst: str, depth: int, source: str) -> None:
        self.edges.add((src, dst, depth, source))

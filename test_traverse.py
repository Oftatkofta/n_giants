from store import MemoryStore
from traverse import Traverser

class FakeOA:
    def __init__(self, by_id):
        self.by_id = by_id
    def get_work(self, oa_id):
        return self.by_id[oa_id]
    def resolve_doi(self, doi):
        return None

def test_traversal_two_hops():
    # Seed cites W2 and W3; W2 cites W4; W3 cites nobody.
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
    st.upsert(__import__("core").WorkNode(key="openalex:W1", oa_id="W1"))
    t = Traverser(st, oa=fake, max_depth=10)
    m = t.run("openalex:W1")
    # nodes reached (excluding seed): W2,W3,W4 => 3
    assert m.counted == 3

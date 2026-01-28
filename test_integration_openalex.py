import os
import pytest
from dotenv import load_dotenv

from clients import OpenAlexClient

pytestmark = pytest.mark.integration

def test_openalex_doi_resolves_and_has_refs():
    load_dotenv()
    api_key = os.getenv("OPENALEX_API_KEY")
    mailto = os.getenv("OPENALEX_MAILTO")
    assert api_key, "OPENALEX_API_KEY missing"

    oa = OpenAlexClient(api_key=api_key, mailto=mailto)

    doi = "10.1128/mbio.00022-22"
    rec = oa.resolve_doi(doi)
    assert rec is not None

    assert "id" in rec and rec["id"].startswith("https://openalex.org/W")

    work = oa.get_work(rec["id"])
    refs = work.get("referenced_works") or []
    assert len(refs) > 0

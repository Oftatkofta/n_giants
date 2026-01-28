from __future__ import annotations
from typing import Any, Optional
import requests
from core import norm_doi, normalize_openalex_id

class OpenAlexClient:
    def __init__(self, api_key: str, mailto: str | None = None):
        self.api_key = api_key
        self.mailto = mailto
        self.s = requests.Session()

    def _params(self, extra: dict[str, Any] | None = None) -> dict[str, Any]:
        p = {"api_key": self.api_key}
        if self.mailto:
            p["mailto"] = self.mailto
        if extra:
            p.update(extra)
        return p

    def resolve_doi(self, doi: str) -> Optional[dict[str, Any]]:
        doi = norm_doi(doi)
        r = self.s.get("https://api.openalex.org/works", params=self._params({"filter": f"doi:{doi}", "per_page": 1}), timeout=30)
        r.raise_for_status()
        res = r.json().get("results") or []
        return res[0] if res else None

    def get_work(self, oa_id: str) -> Optional[dict[str, Any]]:
        oa_id = normalize_openalex_id(oa_id)
        r = self.s.get(f"https://api.openalex.org/works/{oa_id}", params=self._params(), timeout=30)
        if r.status_code in (404, 410):
            return None
        r.raise_for_status()
        return r.json()


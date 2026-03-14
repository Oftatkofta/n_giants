from __future__ import annotations
from typing import Any, Optional
import asyncio
import logging
import requests

try:
    import aiohttp
    HAS_AIOHTTP = True
except ImportError:
    HAS_AIOHTTP = False

from core import norm_doi, normalize_openalex_id

logger = logging.getLogger(__name__)


class OpenAlexClient:
    def __init__(self, api_key: str, mailto: str | None = None, concurrency: int = 30):
        self.api_key = api_key
        self.mailto = mailto
        self.concurrency = concurrency
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

    def get_works_batch(self, oa_ids: list[str]) -> dict[str, Optional[dict[str, Any]]]:
        """
        Fetch multiple works in parallel using aiohttp.
        Returns dict mapping oa_id -> work record (or None if missing/error).
        Falls back to sequential fetching if aiohttp is not available.
        """
        if not oa_ids:
            return {}

        if not HAS_AIOHTTP:
            logger.warning("aiohttp not installed, falling back to sequential fetching")
            return self._get_works_batch_sequential(oa_ids)

        return asyncio.run(self._get_works_batch_async(oa_ids))

    def _get_works_batch_sequential(self, oa_ids: list[str]) -> dict[str, Optional[dict[str, Any]]]:
        """Fallback sequential fetching."""
        results = {}
        for oa_id in oa_ids:
            try:
                results[oa_id] = self.get_work(oa_id)
            except Exception:
                results[oa_id] = None
        return results

    async def _get_works_batch_async(self, oa_ids: list[str]) -> dict[str, Optional[dict[str, Any]]]:
        """Parallel fetch using aiohttp with semaphore for concurrency control."""
        semaphore = asyncio.Semaphore(self.concurrency)
        connector = aiohttp.TCPConnector(limit=self.concurrency, limit_per_host=self.concurrency)
        timeout = aiohttp.ClientTimeout(total=30)

        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            tasks = [self._fetch_one_async(session, semaphore, oa_id) for oa_id in oa_ids]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        out = {}
        for oa_id, result in zip(oa_ids, results):
            if isinstance(result, Exception):
                out[oa_id] = None
            else:
                out[oa_id] = result
        return out

    async def _fetch_one_async(
        self, session: "aiohttp.ClientSession", semaphore: asyncio.Semaphore, oa_id: str
    ) -> Optional[dict[str, Any]]:
        """Fetch a single work with semaphore-controlled concurrency."""
        oa_id = normalize_openalex_id(oa_id)
        url = f"https://api.openalex.org/works/{oa_id}"
        params = self._params()

        async with semaphore:
            try:
                async with session.get(url, params=params) as resp:
                    if resp.status in (404, 410):
                        return None
                    resp.raise_for_status()
                    return await resp.json()
            except Exception:
                return None


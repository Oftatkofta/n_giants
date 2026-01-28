import os
import requests
from dotenv import load_dotenv

load_dotenv()

OPENALEX_BASE = "https://api.openalex.org"

api_key = os.getenv("OPENALEX_API_KEY")
mailto = os.getenv("OPENALEX_MAILTO")

assert api_key, "OPENALEX_API_KEY missing"

params = {
    "api_key": api_key,
    "mailto": mailto,
}

doi = "10.1128/mbio.00022-22"

# Resolve DOI → Work
r = requests.get(
    f"{OPENALEX_BASE}/works",
    params={**params, "filter": f"doi:{doi}", "per_page": 1},
    timeout=30,
)
r.raise_for_status()
data = r.json()
work = data["results"][0]

print("Resolved OpenAlex ID:", work["id"])
print("Type:", work["type"])
print("Year:", work["publication_year"])
print("Number of references:", len(work.get("referenced_works", [])))

# Fetch the same work by ID (single-work endpoint, no select)
oa_id = work["id"].replace("https://openalex.org/", "")
r2 = requests.get(
    f"{OPENALEX_BASE}/works/{oa_id}",
    params=params,
    timeout=30,
)
r2.raise_for_status()
work2 = r2.json()

print("Fetched by ID OK")
print("Referenced works:", len(work2.get("referenced_works", [])))

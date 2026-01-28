import os
from dotenv import load_dotenv
load_dotenv()

SEMANTIC_SCHOLAR_API_KEY = os.getenv("SEMANTIC_SCHOLAR_API_KEY")
OPENALEX_API_KEY = os.getenv("OPENALEX_API_KEY")

print(SEMANTIC_SCHOLAR_API_KEY)
print(OPENALEX_API_KEY)
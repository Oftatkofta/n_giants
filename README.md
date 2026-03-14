# n_giants

**Explore the depth of scientific citation chains** — trace how far back the shoulders of giants extend.

This tool traverses academic citation references using the [OpenAlex API](https://openalex.org/) to discover the deepest and oldest intellectual roots of any research paper.

---

## What is OpenAlex?

[OpenAlex](https://openalex.org/) is a free, open catalog of the global research system. It indexes over 250 million scholarly works, including:

- Journal articles, conference papers, books, and datasets
- Authors, institutions, and research topics
- **Citation relationships** between works

OpenAlex is the successor to Microsoft Academic Graph and provides a REST API for programmatic access to scholarly metadata.

### Why OpenAlex?

| Feature | OpenAlex | Semantic Scholar | Google Scholar |
|---------|----------|------------------|----------------|
| Free API | Yes | Yes (rate-limited) | No |
| Citation data | Yes | Yes | Limited |
| Coverage | 250M+ works | 200M+ works | Unknown |
| Open data | Yes (CC0) | Partial | No |

---

## How It Works

Given a seed paper (via DOI), n_giants recursively follows the **referenced_works** field from OpenAlex to build a citation ancestry tree. The tool supports multiple traversal strategies to explore this tree efficiently.

```
Your Paper (2024)
    └── Reference A (2018)
        └── Reference B (2005)
            └── Reference C (1987)
                └── Reference D (1952)  ← How deep does it go?
```

### Traversal Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| `bfs` | Breadth-first search — explores all references level by level | Complete citation graph mapping |
| `dfs` | Depth-first search — follows one lineage to its end before backtracking | Finding deep citation chains |
| `walk` | Random walks — samples paths stochastically | Statistical depth estimation |
| `promote-longest` | Hybrid — random descent with backtracking to find longer paths | Discovering deepest lineages efficiently |

---

## Installation

### Prerequisites

- Python 3.10+
- An OpenAlex API key (optional but recommended for higher rate limits)

### Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/n_giants.git
   cd n_giants
   ```

2. Install dependencies:
   ```bash
   pip install requests python-dotenv
   ```

3. Create a `.env` file with your credentials:
   ```env
   OPENALEX_API_KEY=your_api_key_here
   OPENALEX_MAILTO=your_email@example.com
   ```

   > **Note:** OpenAlex offers a [polite pool](https://docs.openalex.org/how-to-use-the-api/rate-limits-and-authentication) with higher rate limits if you provide an email address.

---

## Usage

### Basic Example

Trace citations starting from a paper's DOI:

```bash
python run.py --doi "10.1038/nature12373"
```

### Command-Line Options

```bash
python run.py --doi <DOI> [options]

Required:
  --doi             DOI of the seed paper (e.g., "10.1038/nature12373")

General Options:
  --db              SQLite cache file (default: shoulders_cache.sqlite)
  --max-depth       Maximum citation depth to traverse (default: 20)
  --min-year        Ignore papers published before this year (default: 0)
  --mode            Traversal mode: bfs, dfs, walk, promote-longest (default: bfs)

DFS/Hybrid Options:
  --dfs-order       Child ordering: as-listed, year (oldest-first), random
  --dfs-limit       Max nodes to visit (default: 100)
  --stop-on-terminal  Stop after reaching first terminal node
  --record-paths    Output file for terminal paths (JSONL format)
  --paths-top-k     Number of paths to show in summary (default: 10)

Random Walk Options:
  --walks           Number of random walks (default: 10000)
  --walk-seed       RNG seed for reproducibility
  --walk-max-steps  Max steps per walk (default: 5000)

Performance Options (BFS):
  --batch-size      Works to prefetch in parallel (default: 50, 0 to disable)
  --concurrency     Max concurrent API requests (default: 30, requires aiohttp)
```

### Examples

The examples below use DOI `10.1038/s41588-025-02218-x` — a 2025 Nature Genetics paper on gut epithelial organoids.

---

#### DFS Mode (Depth-First, Oldest References First)

```bash
python run.py --doi "10.1038/s41588-025-02218-x" --mode dfs --dfs-order year --dfs-limit 50 --max-depth 10
```

**Output:**
```
=== RUN COMPLETE (DFS) ===
Seed DOI: 10.1038/s41588-025-02218-x
Seed key: openalex:W4411246386
DFS order: year limit=50
Metrics: Metrics(missing_oa_works=4, counted=49, excluded_reviews=0, excluded_preprints=0,
         terminal_no_refs=39, expanded_openalex=12, expanded_s2=0, cache_hit=37, cache_miss=12)
```

---

#### Random Walk Mode (Statistical Depth Estimation)

```bash
python run.py --doi "10.1038/s41588-025-02218-x" --mode walk --walks 100 --walk-max-steps 50
```

**Output:**
```
=== RUN COMPLETE (RANDOM WALKS) ===
Seed DOI: 10.1038/s41588-025-02218-x
Seed key: openalex:W4411246386
Walks: 100
Depth p50/p90/p99/max: 7 12 20 25
Termination reasons: {'openalex-empty': 85, 'openalex-missing': 15}
Cache util: 91.1%
```

This shows the citation depth distribution: median depth of 7, 90th percentile at 12, and maximum depth of 25 references back.

---

#### Promote-Longest Mode (Finding Deepest Lineages)

```bash
python run.py --doi "10.1038/s41588-025-02218-x" --mode promote-longest --dfs-limit 100 --record-paths paths.jsonl --paths-top-k 3
```

**Output:**
```
=== RUN COMPLETE (PROMOTE-LONGEST) ===
Seed DOI: 10.1038/s41588-025-02218-x
Seed key: openalex:W4411246386
Recorded terminal paths to: paths.jsonl

=== DFS PATH SUMMARY ===
Recorded paths: 30
Termination reasons: {'openalex-empty': 29, 'openalex-missing': 1}

--- Top 3 deepest terminal paths ---
#1 depth=9 terminal_year=1961 reason=openalex-empty
 0. openalex:W1971111617 (1988) — Intraindividual variability in the relative systemic availability of cyclosporin...
 1. openalex:W2060392590 (1986) — Pharmacokinetics of oral cyclosporin a (Sandimmun) in healthy subjects
 2. openalex:W2034136287 (1981) — A Radioimmunoassay to Measure Cyclosporin A in Plasma and Serum Samples
 3. openalex:W2077047167 (1977) — Die Struktur von Cyclosporin C
 4. openalex:W2135362208 (1976) — Crystal and Molecular Structure of an Iodo‐derivative of Cyclosporin A
 5. openalex:W2059567258 (1973) — Conformation of twisted β-pleated sheets in proteins
 6. openalex:W2047915087 (1966) — Conformational Analysis of Macromolecules. III. Helical Structures...
 7. openalex:W2163215800 (1966) — The role of van der Waals interactions on conformational stability...
 8. openalex:W2040479471 (1964) — A correlation between amino acid composition and protein structure
 9. openalex:W2015420277 (1961) — A Conformation-dependent Cotton Effect in α-Helical Polypeptides

--- Top 3 oldest terminal-year paths ---
#1 terminal_year=1899 depth=5 reason=openalex-empty
 0. openalex:W1598032254 (1949) — AMINO ACID COMPOSITION OF β-LACTOGLOBULIN AND BOVINE SERUM ALBUMIN
 1. openalex:W175739563 (1944) — THE MICROBIOLOGICAL DETERMINATION OF AMINO ACIDS
 2. openalex:W1795009442 (1940) — THE DETERMINATION OF ARGININE BY MEANS OF FLAVIANIC ACID
 3. openalex:W18952385 (1929) — THE SEPARATION OF CYSTINE FROM HISTIDINE: THE BASIC AMINO ACIDS...
 4. openalex:W2053470872 (1925) — THE OPTICAL ACTIVITY OF CYSTINE
 5. openalex:W2610762087 (1899) — Ueber Drehungsänderungen aktiver Elektrolyte...

#2 terminal_year=1901 depth=6 reason=openalex-empty
 0. openalex:W1612122265 (1950) — AMINO ACID COMPOSITION OF EGG PROTEINS
 ...
 6. openalex:W2171580542 (1901) — A contribution to the chemistry of proteids
```

This reveals fascinating citation chains — from modern organoid research back through immunosuppressant pharmacokinetics, protein crystallography, and all the way to 19th-century biochemistry!

---

#### BFS Mode (Full Citation Graph)

```bash
python run.py --doi "10.1038/s41588-025-02218-x" --mode bfs --max-depth 3
```

**Output (progress logging):**
```
depth= 2 processed=2048 queue=53111 seen=55159 expanded_oa=107 terminal=168 missing_oa=37 new/sec=1648.0 cache=94.7%
```

> **Warning:** BFS explores *all* references at each level before going deeper. Even with `--max-depth 3`, this can expand to tens of thousands of works.
>
> n_giants can provide an **exact** answer (in only 2-3 months), or an **approximate** answer in minutes — if you are not limited by pathological perfectionism and odd fixations.
>
> Use `--max-depth` carefully or prefer DFS/walk modes for exploration.

---

## Output

### Console Logging

The tool logs progress and metrics to both console and `walk.log`:

```
depth= 2 processed=1523 queue=4821 seen=6344 expanded_oa=83 terminal=112 new/sec=1167.4 cache=93.5%
```

**Metrics explained:**
- `depth` — Current traversal depth
- `processed` — Nodes visited so far
- `queue` / `seen` — BFS frontier size / total unique works discovered
- `expanded_oa` — Works fetched from OpenAlex API
- `terminal` — Works with no further references (end of chain)
- `cache` — Cache hit rate (higher = faster, fewer API calls)

### SQLite Cache

All fetched works are cached in a SQLite database (`shoulders_cache.sqlite` by default) containing:

- **works** — Paper metadata (title, year, DOI, type, references)
- **edges** — Citation relationships with depth information

This cache enables fast re-runs and incremental exploration. In the examples above, cache hit rates of 91-95% show how subsequent runs benefit from cached data.

### Path Recording (JSONL)

When using `--record-paths`, terminal paths are saved as JSONL:

```json
{"reason": "openalex-empty", "depth": 9, "path": ["openalex:W4411246386", "openalex:W1971111617", ...]}
```

---

## Visualization & Analysis

### Complete Workflow Example

**Step 1:** Run traversal and record paths to a JSONL file:

```bash
python run.py --doi "10.1038/s41588-025-02218-x" --mode promote-longest --dfs-limit 100 --record-paths paths.jsonl
```

**Step 2:** Explore the recorded paths with `show_paths.py`:

```bash
python show_paths.py --jsonl paths.jsonl --print-mode longest --limit 5
```

---

### show_paths.py — Explore Recorded Paths

The `show_paths.py` script provides rich filtering and visualization of terminal paths recorded with `--record-paths`.

```bash
python show_paths.py --jsonl <paths.jsonl> [options]

Options:
  --db             SQLite cache file (default: shoulders_cache.sqlite)
  --limit          Max paths to display (default: 20)
  --print-mode     Sort by: all, longest, oldest (default: all)
  --min-year       Only show paths ending before this year
  --contains       Filter paths containing keyword in any title
  --csv            Export results to CSV file
```

---

#### Example: Show Longest Citation Chains

```bash
python show_paths.py --jsonl paths.jsonl --print-mode longest --limit 3
```

**Output:**
```
=== Path 1 reason=openalex-empty depth=13 ===
 0. openalex:W4411246386 (2025, article) — A scalable gut epithelial organoid model...
 1. openalex:W4214493147 (2022, article) — The type three secretion system effector protein IpgB1...
 2. openalex:W1997261581 (2013, article) — Enzymatically active Rho and Rac small-GTPases...
 3. openalex:W1602426796 (1996, article) — Rho-dependent membrane folding causes Shigella entry...
 4. openalex:W2334456031 (1991, review) — Genetic and Molecular Basis of Epithelial Cell Invasion...
 5. openalex:W1881868864 (1985, article) — Identification and antigenic characterization...
 6. openalex:W1841532970 (1983, article) — Alterations in the pathogenicity of E. coli K-12...
 7. openalex:W1876016322 (1982, article) — Involvement of a plasmid in the invasive ability...
 8. openalex:W1546818619 (1965, article) — Abortive Intestinal Infection With an E. coli-Shigella Hybrid
 9. openalex:W1622188711 (1963, article) — EXPERIMENTAL SHIGELLA INFECTIONS VI
10. openalex:W1998970463 (1956, article) — EXPERIMENTAL ENTERIC SHIGELLA AND VIBRIO INFECTIONS...
11. openalex:W2047269947 (1955, article) — The Fatal Enteric Cholera Infection in the Guinea Pig...
12. openalex:W2087770430 (1951, article) — An experimental study of the action of cholera toxin
13. openalex:W2327104637 (1940, article) — Acute Circulatory Failure (Shock)...

=== Path 2 reason=openalex-empty depth=3 ===
 0. openalex:W2106159331 (1946, article) — The production and removal of oedema fluid...
 1. openalex:W2091824669 (1943, article) — The blood volume of normal animals
 2. openalex:W2155226425 (1937, article) — CLINICAL STUDIES OF THE BLOOD VOLUME...
 3. openalex:W2886901098 (1920, article) — BLOOD VOLUME STUDIES
```

This 13-step chain traces from 2025 organoid research back through Shigella pathogenesis, plasmid biology (1980s), and early enteric infection studies to a 1940 paper on circulatory shock!

---

#### Example: Filter by Keyword

Find all paths mentioning "Shigella":

```bash
python show_paths.py --jsonl paths.jsonl --contains "shigella" --limit 5
```

**Output:**
```
=== Path 1 reason=openalex-empty depth=13 ===
 0. openalex:W4411246386 (2025, article) — A scalable gut epithelial organoid model...
 1. openalex:W4214493147 (2022, article) — ...IpgB1 promotes Shigella flexneri cell-to-cell spread...
 ...
 7. openalex:W1876016322 (1982, article) — Involvement of a plasmid in the invasive ability of Shigella flexneri
 ...
```

---

#### Example: Export to CSV

Export paths for analysis in Excel, R, or Python:

```bash
python show_paths.py --jsonl paths.jsonl --print-mode oldest --limit 10 --csv paths_export.csv
```

Creates a CSV with columns: `path_index`, `step`, `key`, `year`, `type`, `title`, `reason`, `depth`

---

## Filtering

The tool automatically excludes certain paper types to focus on primary research:

- **Review articles** — Detected via OpenAlex type or title patterns ("review", "meta-analysis", "survey")
- **Preprints** — Detected via venue (arXiv, bioRxiv, medRxiv, etc.)

These filters help trace the chain of *original* contributions rather than secondary literature.

---

## API Reference

### OpenAlex API Endpoints Used

| Endpoint | Purpose |
|----------|---------|
| `GET /works?filter=doi:{doi}` | Resolve DOI to OpenAlex work ID |
| `GET /works/{id}` | Fetch work metadata including `referenced_works` |

### Rate Limits

- **Without authentication:** 10 requests/second
- **With API key:** 100 requests/second
- **Polite pool (with mailto):** Higher limits, priority access

### Parallel Fetching (BFS Mode)

By default, BFS mode uses **parallel batch fetching** via `aiohttp` to maximize throughput:

```bash
# Install aiohttp for parallel fetching
pip install aiohttp

# Recommended settings for maximum throughput
python run.py --doi "..." --mode bfs --batch-size 100 --concurrency 60
```

#### Benchmark Results

Tested with DOI `10.1128/mbio.00022-22` at `--max-depth 2` (~4,300 works):

| Batch Size | Concurrency | Time | Throughput |
|------------|-------------|------|------------|
| 0 (sequential) | 1 | ~18 min | **~4 req/s** |
| 50 | 30 | 68s | **~44 req/s** |
| 100 | 60 | 81s | **~53 req/s** |
| 100 | 100 | 84s | ~51 req/s |

**Key findings:**
- **Concurrency 60** is the sweet spot (~53 req/s)
- Higher concurrency doesn't help (OpenAlex rate limits / network overhead)
- **~13x speedup** over single-threaded mode

#### Real-World Impact

For a large BFS traversal with 14 million queued items (~10% cache miss = 1.4M API calls):

| Mode | Estimated Time |
|------|----------------|
| Sequential (~4 req/s) | **~4 days** |
| Parallel (~53 req/s) | **~7-8 hours** |

Cached results minimize API calls on subsequent runs.

---

## Architecture

```
run.py              # CLI entrypoint for traversal
├── clients.py      # OpenAlex API client
├── core.py         # Data models (WorkNode) and utilities
├── traverse.py     # BFS/DFS/Walk/Hybrid traversal algorithms
├── sqlite_store.py # SQLite-backed caching layer
└── helpers.py      # Logging and path formatting utilities

show_paths.py       # Visualization & analysis of recorded paths
```

---

## Contributing

Contributions are welcome! Areas of interest:

- Additional traversal strategies
- Visualization of citation trees
- Integration with other APIs (Semantic Scholar, Crossref)
- Performance optimizations

---

## License

MIT License — See [LICENSE](LICENSE) for details.

---

## Acknowledgments

- [OpenAlex](https://openalex.org/) for providing open access to scholarly metadata
- Inspired by the idea of tracing the intellectual lineage of scientific discoveries

*"If I have seen further, it is by standing on the shoulders of giants."* — Isaac Newton (1675)

*"How many giants?"* — Jens Eriksson


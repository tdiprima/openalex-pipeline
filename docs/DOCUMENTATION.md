# OpenAlex Pipeline Documentation

## 1. Usage Notes

### Prerequisites

- Python 3.7 or higher
- PostgreSQL database (local or remote)
- Valid email address (required for OpenAlex polite pool access)
- Sufficient database storage for large datasets

### Environment Setup

1. **Create environment file**

   ```bash
   cp .env_sample .env
   ```

2. **Configure environment variables**

   ```env
   DB_USER=your_postgres_username
   DB_PASSWORD=your_postgres_password
   DB_HOST=localhost
   DB_NAME=openalex_data
   OPENALEX_EMAIL=your_email@example.com
   ```

3. **Install dependencies**

   ```bash
   pip install -r requirements.txt
   ```
   or with uv:

   ```bash
   uv pip install -r requirements.txt
   ```

### Running the Pipeline

**Basic usage:**

```bash
python src/openalex_pipeline.py
```

**Customizing parameters:**

Edit the `main()` function in `src/openalex_pipeline.py`:

```python
await pipeline.run(
    max_authors=10000,           # Total authors to fetch
    max_pubs_per_author=10000,   # Publications per author
    concurrency=50               # Parallel processing tasks
)
```

**Recommended settings:**

- Small test run: `max_authors=10, max_pubs_per_author=100, concurrency=5`
- Medium run: `max_authors=1000, max_pubs_per_author=1000, concurrency=25`
- Full institutional crawl: `max_authors=50000, max_pubs_per_author=10000, concurrency=72`

### Monitoring Progress

The pipeline outputs progress information:

```
Fetching authors...
  Fetched batch, total authors so far: 200
  Fetched batch, total authors so far: 400
Found 1000 total authors
Processing authors with concurrency=50...
Processing author 1/1000: John Doe
  ✓ John Doe: 45 publications
Processing author 2/1000: Jane Smith
  ✓ Jane Smith: 127 publications
...
✓ Done! Processed 1000 authors, 82543 total publications
```

### Database Access

After running, query the database:

```sql
-- Count total authors
SELECT COUNT(*) FROM authors;

-- Top 10 most cited authors
SELECT name, cited_by_count
FROM authors
ORDER BY cited_by_count DESC
LIMIT 10;

-- Publications by year
SELECT publication_year, COUNT(*)
FROM publications
GROUP BY publication_year
ORDER BY publication_year DESC;

-- Find publications with PDFs
SELECT title, pdf_url
FROM publications
WHERE pdf_url IS NOT NULL;
```

---

## 2. API Outline

### OpenAlexPipeline Class

**Initialization:**

```python
pipeline = OpenAlexPipeline(db_url: str, email: str)
```

**Parameters:**

- `db_url`: PostgreSQL connection string (format: `postgresql://user:password@host/dbname`)
- `email`: Your email for OpenAlex API polite pool access

**Main Methods:**

#### `connect_db()`
- **Returns:** None
- **Side effects:** Creates connection pool, creates database tables if not exist
- **Raises:** `asyncpg` exceptions if database connection fails

#### `fetch_authors(session, max_results=10000)`
- **Parameters:**
  - `session`: aiohttp.ClientSession
  - `max_results`: Maximum number of authors to fetch
- **Returns:** List[Author]
- **Constraints:** Uses cursor pagination (200 per page), OpenAlex API limits apply
- **Rate limiting:** 0.1s delay between batches

#### `fetch_publications(session, author_id, max_results=10000)`
- **Parameters:**
  - `session`: aiohttp.ClientSession
  - `author_id`: OpenAlex author ID
  - `max_results`: Maximum publications to fetch
- **Returns:** List[Publication]
- **Constraints:** Page-based pagination (200 per page), sorted by publication year descending
- **Rate limiting:** 0.05s delay between pages

#### `save_author(author)`
- **Parameters:** Author dataclass
- **Returns:** None
- **Side effects:** Upserts author to database
- **Constraints:** Uses `ON CONFLICT DO UPDATE` for idempotency

#### `save_publication(pub)`
- **Parameters:** Publication dataclass
- **Returns:** None
- **Side effects:** Upserts publication to database
- **Constraints:** Uses `ON CONFLICT DO UPDATE` for idempotency

#### `process_author(session, author, max_pubs)`
- **Parameters:**
  - `session`: aiohttp.ClientSession
  - `author`: Author dataclass
  - `max_pubs`: Maximum publications per author
- **Returns:** int (number of publications saved)
- **Side effects:** Saves author and all publications

#### `run(max_authors=10000, max_pubs_per_author=10000, concurrency=50)`
- **Parameters:**
  - `max_authors`: Total authors to process
  - `max_pubs_per_author`: Publications per author limit
  - `concurrency`: Number of parallel author processing tasks
- **Returns:** None
- **Side effects:** Fetches all data, saves to database, closes connection pool
- **Constraints:** Semaphore controls concurrency to prevent overwhelming API/database

### Data Models

#### Author

```python
@dataclass
class Author:
    id: str                      # OpenAlex author ID (max 500 chars)
    name: str                    # Display name (max 500 chars)
    works_count: int             # Total works count
    cited_by_count: int          # Total citations
    affiliations: List[str]      # Institution names (each max 500 chars)
```

#### Publication

```python
@dataclass
class Publication:
    id: str                      # OpenAlex work ID (max 500 chars)
    title: str                   # Publication title (max 1000 chars)
    doi: Optional[str]           # DOI if available (max 500 chars)
    publication_year: int        # Year of publication
    pdf_url: Optional[str]       # PDF URL if available (max 1000 chars)
    authors: List[str]           # Author names (each max 500 chars)
    abstract: Optional[str]      # String representation of inverted index (max 5000 chars)
```

### Database Schema

**Authors table:**

```sql
CREATE TABLE authors (
    id TEXT PRIMARY KEY,
    name TEXT,
    works_count INT,
    cited_by_count INT,
    affiliations TEXT[]
);
```

**Publications table:**

```sql
CREATE TABLE publications (
    id TEXT PRIMARY KEY,
    title TEXT,
    doi TEXT,
    publication_year INT,
    pdf_url TEXT,
    authors TEXT[],
    abstract TEXT
);
```

### API Constraints

**OpenAlex API:**

- Base URL: `https://api.openalex.org`
- Requires `mailto` parameter for polite pool access
- Cursor pagination for authors (recommended for large result sets)
- Page-based pagination for publications
- Rate limiting: Pipeline adds delays (0.05-0.1s) to be respectful
- Free tier with no API key required

**Database:**

- Connection pool: 10-100 connections
- Command timeout: 60 seconds
- SSL: Disabled (configure based on your environment)
- PostgreSQL array types used for multi-valued fields

---

## 3. Decision Notes

### Architectural Decisions

#### Why Async/Await?
- **I/O-bound workload:** The pipeline spends most time waiting for API responses and database writes
- **Massive parallelism:** Processing thousands of authors concurrently dramatically reduces total runtime
- **Efficient resource usage:** Single-threaded async handles concurrency without multiprocessing overhead
- **Natural fit:** `aiohttp` for HTTP requests, `asyncpg` for PostgreSQL, native Python async

#### Why PostgreSQL Arrays?
- **Normalized yet pragmatic:** Affiliations and authors are multi-valued but don't need separate tables
- **Query flexibility:** Can search within arrays using PostgreSQL operators
- **Storage efficiency:** Avoids join overhead for common queries
- **Simple schema:** Reduces complexity for this use case

#### Why Connection Pooling?
- **Concurrency support:** Multiple async tasks need simultaneous database access
- **Resource management:** Prevents exhausting database connections
- **Performance:** Reusing connections avoids overhead of repeated authentication
- **Configuration:** 10-100 connection pool size matches high concurrency (72 tasks)

### Data Handling Decisions

#### String Truncation
All strings are truncated to safe maximums:

- **Purpose:** Prevent database errors from unexpectedly long data
- **Implementation:** Applied during data extraction from API responses
- **Tradeoffs:** Rare data loss vs. pipeline reliability (reliability prioritized)

#### Abstract Storage as String
- **OpenAlex format:** Abstracts provided as inverted index (dict of word positions)
- **Decision:** Store string representation rather than reconstructing full text
- **Rationale:**
  - Preserves original data structure
  - Reconstruction is complex and error-prone
  - Users can implement custom reconstruction if needed
- **Limit:** 5000 characters prevents excessive storage

#### Upsert Strategy (ON CONFLICT DO UPDATE)
- **Idempotency:** Re-running pipeline won't create duplicates
- **Updates:** Captures data changes if re-run later
- **Safety:** Can stop and restart pipeline without data loss
- **Performance:** Slightly slower than INSERT-only, but worth the reliability

### Performance Decisions

#### Concurrency Level
- **Default:** 50 concurrent tasks
- **High-performance:** 72 tasks (matches server in comment at line 299)
- **Rationale:**
  - OpenAlex API can handle high request rates
  - PostgreSQL pool supports up to 100 connections
  - Semaphore prevents overwhelming either service
- **Tuning:** Adjust based on network bandwidth and database capacity

#### Pagination Strategy
- **Authors:** Cursor-based pagination
  - Better for large result sets
  - More stable when data changes during fetching
  - OpenAlex recommendation for production use
- **Publications:** Page-based pagination
  - Simpler implementation
  - Adequate for per-author queries (usually <10k results)
  - Sorted by year (most recent first) prioritizes recent work

#### Rate Limiting
- **Author batches:** 0.1s delay
- **Publication pages:** 0.05s delay
- **Purpose:** Be respectful to OpenAlex API (polite pool etiquette)
- **Impact:** Minimal given high concurrency
- **Benefit:** Reduces risk of rate limiting or IP blocking

### Data Quality Decisions

#### Filtering Institution
- **ROR:** `05qghxh33` (Stony Brook University)
- **Filter level:** Affiliations (not current affiliation)
- **Implication:** Captures anyone ever affiliated with Stony Brook
- **Rationale:** More comprehensive institutional impact analysis

#### Publication Sorting
- **Sort:** `publication_year:desc`
- **Rationale:** Recent work often most relevant
- **Impact:** If hitting `max_pubs_per_author`, keeps newest publications
- **Alternative considered:** Sort by citations (decided recent work more valuable)

#### Field Selection
- **Minimal fields:** Only essential metadata stored
- **Omitted:** MeSH terms, concepts, referenced works, etc.
- **Rationale:** Reduces storage, simplifies schema, focuses on core use case
- **Extensibility:** Schema easily extended if additional fields needed later

### Error Handling Decisions

#### Command Timeout
- **Set:** 60 seconds for database operations
- **Purpose:** Prevent hung queries from blocking the pipeline
- **Tradeoff:** Very large batch inserts might timeout (unlikely in this design)

#### No Retry Logic
- **Current:** Failures propagate immediately
- **Rationale:**
  - Async gather will fail fast on first error
  - Better to fix root cause than retry indefinitely
  - Upsert strategy makes manual re-runs safe
- **Future consideration:** Could add retry with exponential backoff for transient API errors

#### Optional Fields
- `doi`, `pdf_url`, `abstract` are Optional[str]
- **Rationale:** Not all publications have these fields in OpenAlex
- **Implementation:** NULL in database when missing
- **Queries:** Must handle NULL appropriately

### Design Tradeoffs

#### No Author-Publication Join Table
- **Current:** Publications store author list as array
- **Alternative:** Separate junction table for many-to-many relationship
- **Decision:** Array approach chosen for:
  - Simpler schema
  - Faster queries for "show publication with authors"
  - Adequate for this analytics use case
- **Limitation:** Harder to query "find all publications by author X" across stored data
  - Mitigated by: Query uses OpenAlex as source of truth, not local database

#### No Incremental Updates
- **Current:** Full re-fetch on each run
- **Alternative:** Track last update timestamp, fetch only new/changed records
- **Decision:** Full fetch chosen for:
  - Simplicity
  - OpenAlex data changes (citation counts update)
  - Upsert handles duplicates efficiently
- **Consideration:** For very large datasets (>100k authors), incremental approach may be necessary

#### Hard-coded Institution
- **Current:** Stony Brook ROR hard-coded in class
- **Alternative:** Pass as parameter
- **Decision:** Hard-coded for:
  - This is a single-institution tool
  - Simplifies usage
- **Extensibility:** Easy to parameterize later if needed

---

## Summary

This pipeline prioritizes **reliability**, **simplicity**, and **performance** for bulk data extraction from OpenAlex. Async architecture enables high-concurrency processing while remaining respectful to the API. The PostgreSQL schema balances normalization with pragmatism, using arrays for multi-valued fields to simplify common queries. Upsert strategy ensures idempotent runs, making the pipeline safe to stop/restart. All design decisions favor getting researchers' publication data quickly and reliably into a queryable database.

<br>

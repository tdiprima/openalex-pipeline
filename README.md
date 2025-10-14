# OpenAlex Pipeline

An async Python pipeline to fetch Stony Brook University authors and their publications from OpenAlex API and store them in PostgreSQL.

## Features

- Fetches authors affiliated with Stony Brook University (ROR: `05qghxh33`)
- Retrieves publications for each author
- Stores data in PostgreSQL with automatic schema creation
- Async/await for efficient API requests
- Rate limiting to respect API guidelines

## Requirements

- Python 3.7+
- PostgreSQL database
- Environment variables configured (see Setup)

## Dependencies

- `asyncio` - Async runtime
- `aiohttp` - Async HTTP client
- `asyncpg` - Async PostgreSQL driver
- `python-dotenv` - Environment variable management

## Setup

1. Create a `.env` file with the following variables:

```env
DB_USER=your_db_user
DB_PASSWORD=your_db_password
DB_HOST=localhost
DB_NAME=your_db_name
OPENALEX_EMAIL=your_email@example.com
```

2. Install dependencies:

```bash
pip install aiohttp asyncpg python-dotenv
```

## Usage

Run the pipeline:

```bash
python src/openalex_pipeline.py
```

The pipeline will:
1. Connect to PostgreSQL and create tables if they don't exist
2. Fetch up to 50 authors from Stony Brook (sorted by citation count)
3. For each author, fetch up to 100 publications (sorted by year)
4. Store all data in the database

## Database Schema

### Authors Table

```sql
CREATE TABLE authors (
    id TEXT PRIMARY KEY,
    name TEXT,
    works_count INT,
    cited_by_count INT,
    affiliations TEXT[]
)
```

### Publications Table

```sql
CREATE TABLE publications (
    id TEXT PRIMARY KEY,
    title TEXT,
    doi TEXT,
    publication_year INT,
    pdf_url TEXT,
    authors TEXT[],
    abstract TEXT
)
```

## Configuration

Modify the pipeline parameters in `main()`:

```python
await pipeline.run(
    max_authors=50,           # Maximum authors to fetch
    max_pubs_per_author=100   # Maximum publications per author
)
```

## Code Structure

- `Author` - Dataclass for author data
- `Publication` - Dataclass for publication data
- `OpenAlexPipeline` - Main pipeline class with methods:
  - `connect_db()` - Initialize database connection and tables
  - `fetch_authors()` - Query OpenAlex for Stony Brook authors
  - `fetch_publications()` - Query OpenAlex for author publications
  - `save_author()` - Insert/update author in database
  - `save_publication()` - Insert/update publication in database
  - `run()` - Execute the complete pipeline

## API Details

- Base URL: `https://api.openalex.org`
- Uses polite pool (requires email in `mailto` parameter)
- Rate limited with 0.1s delay between author processing
- Results sorted by relevance (citations for authors, year for publications)

## Notes

- The pipeline uses `ON CONFLICT DO UPDATE` for idempotent inserts
- Abstract is stored as inverted index from OpenAlex
- Affiliations are stored as PostgreSQL array type
- PDF URLs are extracted from primary location when available

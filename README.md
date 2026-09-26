# openalex-pipeline

Async Python pipeline that ingests OpenAlex author and publication data into PostgreSQL with configurable concurrency.

## Tens of Thousands of Authors, Millions of Publications, One Paginated API

[OpenAlex](https://openalex.org/) is a free, open catalog of the global research system. It covers authors, publications, institutions, and citations across every academic discipline. The data is there, but accessing it at institutional scale means navigating a paginated REST API with rate limits and no bulk export. A naive sequential approach for a large research university can take hours.

For a large institution such as ExampleOrg, that can be 40,000+ authors and potentially millions of publications locked behind cursor-paginated endpoints returning 200 records at a time.

## Parallel Ingestion with Async I/O

This pipeline automates the full extraction: fetch every affiliated author, pull all their publications, and land everything in PostgreSQL where it can be queried directly. It uses Python's `asyncio` with `aiohttp` for concurrent HTTP requests and `asyncpg` for fast database writes, controlled by a semaphore so you can tune concurrency without hammering the API.

- **Cursor pagination** walks the full author list without skipping or duplicating records
- **Semaphore-based concurrency** lets you scale from 5 requests (testing) to 72+ (production) with a single parameter
- **Upsert logic** means re-running the pipeline updates existing records instead of creating duplicates
- **Auto-created tables** so there's no schema migration step on first run

## Example

Start small to verify everything works:

```python
await pipeline.run(max_authors=10, max_pubs_per_author=100, concurrency=5)
```

Then scale to the full dataset:

```python
await pipeline.run(max_authors=40866, max_pubs_per_author=10000, concurrency=72)
```

Sample output:

```
2025-05-10 14:23:01 INFO __main__ Fetching authors
2025-05-10 14:23:08 INFO __main__ Authors fetched    count=40866
2025-05-10 14:23:08 INFO __main__ Processing authors  concurrency=72
2025-05-10 14:23:09 INFO __main__ Processing author   index=1 total=40866 author=John Smith
2025-05-10 14:23:10 INFO __main__ Author complete     author=John Smith publications=47
...
2025-05-10 16:41:33 INFO __main__ Pipeline complete   authors=40866 publications=1284523
```

## Usage

### Prerequisites

- Python 3.9+
- PostgreSQL

### Install dependencies

```bash
pip install -r requirements.txt
```

### Configure environment

```bash
cp .env_sample .env
```

Edit `.env` with your credentials:

```
DB_USER=your_user
DB_PASSWORD=your_password
DB_HOST=localhost
DB_NAME=your_dbname
OPENALEX_EMAIL=your@email.com
INSTITUTION_ROR=00000000a
```

Create the database specified in `DB_NAME`. Tables are created automatically on first run.

### Run the pipeline

```bash
python src/openalex_pipeline.py
```

### Utility scripts

**Count total institution authors in OpenAlex** (single API call, useful for setting `max_authors`):

```bash
python src/utils/count_authors.py
```

**Export publications for a list of authors** from the database to CSV:

```bash
python src/utils/check_profiles.py
```

**Search PubMed for author affiliations** (requires `NCBI_API_KEY` in `.env` for higher rate limits):

```bash
python src/utils/pubmed_author_search.py
```

## License

[MIT](LICENSE)

<BR>

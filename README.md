# openalex-pipeline

Pulls Stony Brook author + publication data from OpenAlex into PostgreSQL.

## What this does

1. Gets Stony Brook authors from OpenAlex.
2. Gets publications for each author.
3. Saves everything in your Postgres database.

## 5-minute setup

### 1) Install Python packages

```bash
pip install -r requirements.txt
```

### 2) Create your env file

```bash
cp .env_sample .env
```

Then edit `.env`:

```env
DB_USER=your_user
DB_PASSWORD=your_password
DB_HOST=localhost
DB_NAME=your_dbname
OPENALEX_EMAIL=your@email.com
```

### 3) Make sure Postgres DB exists

Create the database named in `DB_NAME`.

Note: tables are auto-created by the script.

## Run it

```bash
python src/openalex_pipeline.py
```

That is the main command.

## Important default

`src/openalex_pipeline.py` currently runs with a **large** job size:

- `max_authors=40866`
- `max_pubs_per_author=10000`
- `concurrency=72`

If you want a small test first, edit `main()` in `src/openalex_pipeline.py` and use something like:

```python
await pipeline.run(max_authors=10, max_pubs_per_author=100, concurrency=5)
```

## Useful extra scripts

- Count total Stony Brook authors in OpenAlex:

```bash
python src/count_authors.py
```

- Export publications for authors listed in `authors_with_pubs_found.csv`:

```bash
python src/check_profiles.py
```

- PubMed search helper (requires editing config constants in `src/pubmed_author_search.py` before running):

```bash
python src/pubmed_author_search.py
```

<br>

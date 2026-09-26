import argparse
import asyncio
import json
import logging
import os
from dataclasses import dataclass
from typing import Iterable, List, Optional
from urllib.parse import quote

import aiohttp
import asyncpg
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)


@dataclass
class Author:
    id: str
    name: str
    works_count: int
    cited_by_count: int
    affiliations: List[str]


@dataclass
class Publication:
    id: str
    title: str
    doi: Optional[str]
    publication_year: int
    pdf_url: Optional[str]
    authors: List[str]
    author_ids: List[str]
    abstract: Optional[str]


class OpenAlexFetchError(RuntimeError):
    """Raised when the OpenAlex API returns a non-success response."""


class OpenAlexPipeline:
    BASE_URL = "https://api.openalex.org"

    def __init__(self, db_url: str, email: str, institution_ror: str):
        self.db_url = db_url
        self.email = email
        self.institution_ror = institution_ror
        self.pool = None

    async def connect_db(self):
        """Create PostgreSQL connection pool."""
        self.pool = await asyncpg.create_pool(
            self.db_url,
            ssl=False,
            min_size=10,
            command_timeout=60,
            max_size=100,
        )

    SCHEMA_VERSION = 2

    # Ordered, idempotent migrations keyed by target schema version.
    MIGRATIONS = {
        1: [
            """
            CREATE TABLE IF NOT EXISTS authors (
                id TEXT PRIMARY KEY,
                name TEXT,
                works_count INT,
                cited_by_count INT,
                affiliations TEXT[]
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS publications (
                id TEXT PRIMARY KEY,
                title TEXT,
                doi TEXT,
                publication_year INT,
                pdf_url TEXT,
                authors TEXT[],
                abstract TEXT
            )
            """,
        ],
        2: [
            "ALTER TABLE publications ADD COLUMN IF NOT EXISTS author_ids TEXT[]",
            # ARCH-5: array-overlap lookups on author_ids use a GIN index
            """
            CREATE INDEX IF NOT EXISTS publications_author_ids_gin
                ON publications USING GIN (author_ids)
            """,
            """
            CREATE TABLE IF NOT EXISTS ingestion_runs (
                id SERIAL PRIMARY KEY,
                started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                finished_at TIMESTAMPTZ,
                status TEXT NOT NULL DEFAULT 'running',
                authors_total INT,
                authors_completed INT,
                error TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS author_ingestion_status (
                author_id TEXT PRIMARY KEY REFERENCES authors(id),
                run_id INT REFERENCES ingestion_runs(id),
                status TEXT NOT NULL,
                publications_count INT,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """,
        ],
    }

    async def migrate(self):
        """Apply schema migrations explicitly and record the schema version."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_version (
                    version INT PRIMARY KEY,
                    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            current = await conn.fetchval(
                "SELECT COALESCE(MAX(version), 0) FROM schema_version"
            )
            for version in sorted(self.MIGRATIONS):
                if version <= current:
                    continue
                logger.info("Applying schema migration", extra={"version": version})
                async with conn.transaction():
                    for stmt in self.MIGRATIONS[version]:
                        await conn.execute(stmt)
                    await conn.execute(
                        "INSERT INTO schema_version (version) VALUES ($1)", version
                    )

    # Backwards-compatible alias
    create_tables = migrate

    async def start_run(self, authors_total: Optional[int] = None) -> int:
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                "INSERT INTO ingestion_runs (authors_total) VALUES ($1) RETURNING id",
                authors_total,
            )

    async def finish_run(
        self, run_id: int, status: str, completed: int = 0, error: str = None
    ):
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE ingestion_runs
                SET finished_at = now(), status = $2, authors_completed = $3, error = $4
                WHERE id = $1
                """,
                run_id,
                status,
                completed,
                error,
            )

    async def set_author_status(
        self, author_id: str, run_id: int, status: str, pub_count: int = None
    ):
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO author_ingestion_status
                    (author_id, run_id, status, publications_count, updated_at)
                VALUES ($1, $2, $3, $4, now())
                ON CONFLICT (author_id) DO UPDATE SET
                    run_id = EXCLUDED.run_id,
                    status = EXCLUDED.status,
                    publications_count = EXCLUDED.publications_count,
                    updated_at = now()
                """,
                author_id,
                run_id,
                status,
                pub_count,
            )

    async def completed_author_ids(self) -> set:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT author_id FROM author_ingestion_status WHERE status = 'complete'"
            )
        return {r["author_id"] for r in rows}

    async def fetch_authors(
        self, session: aiohttp.ClientSession, max_results: int = 10000
    ) -> List[Author]:
        """Fetch authors from the configured institution using cursor pagination."""
        authors = []
        per_page = 200
        cursor = "*"

        while len(authors) < max_results:
            url = f"{self.BASE_URL}/authors"
            params = {
                "filter": f"affiliations.institution.ror:{self.institution_ror}",
                "per-page": per_page,
                "cursor": cursor,
                "mailto": self.email,
            }

            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    logger.error(
                        "Failed to fetch authors",
                        extra={"status": resp.status, "url": url},
                    )
                    raise OpenAlexFetchError(
                        f"Failed to fetch authors: HTTP {resp.status}"
                    )

                data = await resp.json()
                results = data.get("results", [])
                meta = data.get("meta", {})

                if not results:
                    break

                for item in results:
                    if len(authors) >= max_results:
                        break
                    author = Author(
                        id=item["id"][:500],
                        name=item.get("display_name", "")[:500],
                        works_count=item.get("works_count", 0),
                        cited_by_count=item.get("cited_by_count", 0),
                        affiliations=[
                            (aff.get("institution") or {}).get("display_name", "")[:500]
                            for aff in item.get("affiliations", [])
                        ],
                    )
                    authors.append(author)

                logger.info("Fetched batch", extra={"total_authors": len(authors)})

                next_cursor = meta.get("next_cursor")
                if not next_cursor or len(authors) >= max_results:
                    break

                cursor = next_cursor
                await asyncio.sleep(0.1)

        return authors

    async def fetch_publications(
        self, session: aiohttp.ClientSession, author_id: str, max_results: int = 10000
    ) -> List[Publication]:
        """Fetch publications for an author."""
        pubs = []
        page = 1
        per_page = 200

        while len(pubs) < max_results:
            url = f"{self.BASE_URL}/works"
            params = {
                "filter": f"authorships.author.id:{author_id}",
                "per-page": per_page,
                "page": page,
                "sort": "publication_year:desc",
                "mailto": self.email,
            }

            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    logger.error(
                        "Failed to fetch publications",
                        extra={"status": resp.status, "author_id": author_id},
                    )
                    raise OpenAlexFetchError(
                        f"Failed to fetch publications for {author_id}: "
                        f"HTTP {resp.status}"
                    )

                data = await resp.json()
                results = data.get("results", [])

                if not results:
                    break

                for item in results:
                    abstract = None
                    if item.get("abstract_inverted_index"):
                        abstract = json.dumps(item["abstract_inverted_index"])

                    pdf_url = None
                    primary_location = item.get("primary_location") or {}
                    if primary_location.get("pdf_url"):
                        pdf_url = primary_location["pdf_url"][:1000]

                    pub = Publication(
                        id=item["id"][:500],
                        title=(item.get("title") or "")[:1000],
                        doi=item.get("doi", "")[:500] if item.get("doi") else None,
                        publication_year=item.get("publication_year", 0),
                        pdf_url=pdf_url,
                        authors=[
                            (a.get("author") or {}).get("display_name", "")[:500]
                            for a in item.get("authorships", [])
                        ],
                        author_ids=[
                            (a.get("author") or {}).get("id", "")[:500]
                            for a in item.get("authorships", [])
                        ],
                        abstract=abstract,
                    )
                    pubs.append(pub)

                if len(results) < per_page:
                    break

                page += 1
                await asyncio.sleep(0.05)

        return pubs[:max_results]

    async def save_author(self, author: Author):
        """Upsert an author record into the database."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO authors (id, name, works_count, cited_by_count, affiliations)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    works_count = EXCLUDED.works_count,
                    cited_by_count = EXCLUDED.cited_by_count,
                    affiliations = EXCLUDED.affiliations
                """,
                author.id,
                author.name,
                author.works_count,
                author.cited_by_count,
                author.affiliations,
            )

    async def save_publication(self, pub: Publication):
        """Upsert a publication record into the database."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO publications
                    (id, title, doi, publication_year, pdf_url, authors, author_ids, abstract)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (id) DO UPDATE SET
                    title = EXCLUDED.title,
                    doi = EXCLUDED.doi,
                    publication_year = EXCLUDED.publication_year,
                    pdf_url = EXCLUDED.pdf_url,
                    authors = EXCLUDED.authors,
                    author_ids = EXCLUDED.author_ids,
                    abstract = EXCLUDED.abstract
                """,
                pub.id,
                pub.title,
                pub.doi,
                pub.publication_year,
                pub.pdf_url,
                pub.authors,
                pub.author_ids,
                pub.abstract,
            )

    async def process_author(
        self,
        session: aiohttp.ClientSession,
        author: Author,
        max_pubs: int,
        run_id: Optional[int] = None,
    ) -> int:
        """Save an author and fetch and save all their publications.

        Per-author completion is persisted so a restart can skip finished
        authors and so exports can tell incomplete ingestion from missing data.
        """
        await self.save_author(author)
        if run_id is not None:
            await self.set_author_status(author.id, run_id, "in_progress")
        try:
            pubs = await self.fetch_publications(session, author.id, max_pubs)
            for pub in pubs:
                await self.save_publication(pub)
        except BaseException:
            if run_id is not None:
                await self.set_author_status(author.id, run_id, "failed")
            raise
        if run_id is not None:
            await self.set_author_status(author.id, run_id, "complete", len(pubs))
        return len(pubs)

    async def _process_with_semaphore(
        self,
        semaphore: asyncio.Semaphore,
        session: aiohttp.ClientSession,
        index: int,
        author: Author,
        total: int,
        max_pubs: int,
        run_id: Optional[int] = None,
    ) -> int:
        """Wrap process_author with a semaphore for concurrency control."""
        async with semaphore:
            logger.info(
                "Processing author",
                extra={"index": index + 1, "total": total, "author": author.name},
            )
            pub_count = await self.process_author(session, author, max_pubs, run_id)
            logger.info(
                "Author complete",
                extra={"author": author.name, "publications": pub_count},
            )
            return pub_count

    async def run(
        self,
        max_authors: int = 10000,
        max_pubs_per_author: int = 10000,
        concurrency: int = 50,
        resume: bool = True,
    ):
        """Orchestrate the full pipeline: fetch authors, then their publications.

        Raises on any fetch failure so that partial ingestion is never reported
        as complete. Completion is persisted in ingestion_runs and
        author_ingestion_status; with resume=True, authors already marked
        complete by a previous run are skipped.
        """
        await self.connect_db()
        run_id = None
        try:
            await self.migrate()

            async with aiohttp.ClientSession() as session:
                logger.info("Fetching authors")
                authors = await self.fetch_authors(session, max_authors)
                logger.info("Authors fetched", extra={"count": len(authors)})

                already_done = await self.completed_author_ids() if resume else set()
                pending = [a for a in authors if a.id not in already_done]
                logger.info(
                    "Resume check",
                    extra={"skipped": len(authors) - len(pending), "pending": len(pending)},
                )

                run_id = await self.start_run(authors_total=len(authors))

                logger.info("Processing authors", extra={"concurrency": concurrency})
                semaphore = asyncio.Semaphore(concurrency)

                tasks = [
                    asyncio.create_task(
                        self._process_with_semaphore(
                            semaphore,
                            session,
                            index,
                            author,
                            len(pending),
                            max_pubs_per_author,
                            run_id,
                        )
                    )
                    for index, author in enumerate(pending)
                ]
                try:
                    results = await asyncio.gather(*tasks)
                except BaseException as exc:
                    for task in tasks:
                        task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
                    completed = len(await self.completed_author_ids())
                    await self.finish_run(run_id, "failed", completed, repr(exc))
                    logger.error("Pipeline failed; ingestion is incomplete")
                    raise

                total_pubs = sum(results)
                await self.finish_run(run_id, "complete", len(authors))
                logger.info(
                    "Pipeline complete",
                    extra={"authors": len(authors), "publications": total_pubs},
                )
        finally:
            await self.pool.close()

    async def backfill_author_ids(self, batch_size: int = 50):
        """Populate author_ids for publications ingested before schema v2.

        Works are re-fetched from OpenAlex in batches by ID; only the
        author_ids column is updated.
        """
        await self.connect_db()
        try:
            await self.migrate()
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT id FROM publications WHERE author_ids IS NULL"
                )
            ids = [r["id"] for r in rows]
            logger.info("Backfill starting", extra={"publications": len(ids)})
            if not ids:
                return

            async with aiohttp.ClientSession() as session:
                for start in range(0, len(ids), batch_size):
                    batch = ids[start : start + batch_size]
                    await self._backfill_batch(session, batch)
                    logger.info(
                        "Backfill progress",
                        extra={"done": min(start + batch_size, len(ids)), "total": len(ids)},
                    )
                    await asyncio.sleep(0.1)

            async with self.pool.acquire() as conn:
                remaining = await conn.fetchval(
                    "SELECT COUNT(*) FROM publications WHERE author_ids IS NULL"
                )
            logger.info("Backfill complete", extra={"still_null": remaining})
        finally:
            await self.pool.close()

    async def _backfill_batch(self, session: aiohttp.ClientSession, ids: Iterable[str]):
        short_ids = [i.rsplit("/", 1)[-1] for i in ids]
        params = {
            "filter": f"ids.openalex:{'|'.join(short_ids)}",
            "per-page": len(short_ids),
            "select": "id,authorships",
            "mailto": self.email,
        }
        async with session.get(f"{self.BASE_URL}/works", params=params) as resp:
            if resp.status != 200:
                raise OpenAlexFetchError(f"Backfill fetch failed: HTTP {resp.status}")
            data = await resp.json()

        updates = []
        for item in data.get("results", []):
            author_ids = [
                (a.get("author") or {}).get("id", "")[:500]
                for a in item.get("authorships", [])
            ]
            updates.append((item["id"][:500], author_ids))

        async with self.pool.acquire() as conn:
            await conn.executemany(
                "UPDATE publications SET author_ids = $2 WHERE id = $1", updates
            )


def load_config() -> dict:
    """Load and validate required configuration from environment."""
    required = ["DB_USER", "DB_PASSWORD", "DB_NAME", "OPENALEX_EMAIL", "INSTITUTION_ROR"]
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise EnvironmentError(f"Missing required environment variables: {missing}")

    return {
        "db_user": os.environ["DB_USER"],
        "db_password": os.environ["DB_PASSWORD"],
        "db_host": os.getenv("DB_HOST", "localhost"),
        "db_name": os.environ["DB_NAME"],
        "email": os.environ["OPENALEX_EMAIL"],
        "institution_ror": os.environ["INSTITUTION_ROR"],
    }


async def main():
    parser = argparse.ArgumentParser(description="OpenAlex ingestion pipeline")
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Only populate author_ids for publications ingested before schema v2",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Re-process authors already marked complete by an earlier run",
    )
    args = parser.parse_args()

    config = load_config()
    db_url = (
        f"postgresql://{config['db_user']}:{quote(config['db_password'], safe='')}"
        f"@{config['db_host']}/{config['db_name']}"
    )
    pipeline = OpenAlexPipeline(db_url, config["email"], config["institution_ror"])

    if args.backfill:
        await pipeline.backfill_author_ids()
        return

    # With 72 cores, use high concurrency
    await pipeline.run(
        max_authors=40866,
        max_pubs_per_author=10000,
        concurrency=72,
        resume=not args.no_resume,
    )


if __name__ == "__main__":
    asyncio.run(main())

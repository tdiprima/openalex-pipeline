import asyncio
import logging
import os
from dataclasses import dataclass
from itertools import starmap
from typing import List, Optional
from urllib.parse import quote_plus

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
    abstract: Optional[str]


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

    async def create_tables(self):
        """Create required tables if they do not exist."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS authors (
                    id TEXT PRIMARY KEY,
                    name TEXT,
                    works_count INT,
                    cited_by_count INT,
                    affiliations TEXT[]
                )
                """
            )
            await conn.execute(
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
                """
            )

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
                    break

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
                            aff.get("display_name", "")[:500]
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
                    break

                data = await resp.json()
                results = data.get("results", [])

                if not results:
                    break

                for item in results:
                    abstract = None
                    if item.get("abstract_inverted_index"):
                        abstract = str(item.get("abstract_inverted_index"))[:5000]

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
                            a.get("author", {}).get("display_name", "")[:500]
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
                INSERT INTO publications (id, title, doi, publication_year, pdf_url, authors, abstract)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (id) DO UPDATE SET
                    title = EXCLUDED.title,
                    doi = EXCLUDED.doi,
                    publication_year = EXCLUDED.publication_year,
                    pdf_url = EXCLUDED.pdf_url,
                    authors = EXCLUDED.authors,
                    abstract = EXCLUDED.abstract
                """,
                pub.id,
                pub.title,
                pub.doi,
                pub.publication_year,
                pub.pdf_url,
                pub.authors,
                pub.abstract,
            )

    async def process_author(
        self, session: aiohttp.ClientSession, author: Author, max_pubs: int
    ) -> int:
        """Save an author and fetch and save all their publications."""
        await self.save_author(author)
        pubs = await self.fetch_publications(session, author.id, max_pubs)
        for pub in pubs:
            await self.save_publication(pub)
        return len(pubs)

    async def _process_with_semaphore(
        self,
        semaphore: asyncio.Semaphore,
        session: aiohttp.ClientSession,
        index: int,
        author: Author,
        total: int,
        max_pubs: int,
    ) -> int:
        """Wrap process_author with a semaphore for concurrency control."""
        async with semaphore:
            logger.info(
                "Processing author",
                extra={"index": index + 1, "total": total, "author": author.name},
            )
            pub_count = await self.process_author(session, author, max_pubs)
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
    ):
        """Orchestrate the full pipeline: fetch authors, then their publications."""
        await self.connect_db()
        await self.create_tables()

        async with aiohttp.ClientSession() as session:
            logger.info("Fetching authors")
            authors = await self.fetch_authors(session, max_authors)
            logger.info("Authors fetched", extra={"count": len(authors)})

            logger.info("Processing authors", extra={"concurrency": concurrency})
            semaphore = asyncio.Semaphore(concurrency)

            tasks = [
                self._process_with_semaphore(
                    semaphore, session, index, author, len(authors), max_pubs_per_author
                )
                for index, author in enumerate(authors)
            ]
            results = await asyncio.gather(*tasks)

            total_pubs = sum(results)
            logger.info(
                "Pipeline complete",
                extra={"authors": len(authors), "publications": total_pubs},
            )

        await self.pool.close()


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
    config = load_config()
    db_url = (
        f"postgresql://{config['db_user']}:{quote_plus(config['db_password'])}"
        f"@{config['db_host']}/{config['db_name']}"
    )
    pipeline = OpenAlexPipeline(db_url, config["email"], config["institution_ror"])
    # With 72 cores, use high concurrency
    await pipeline.run(max_authors=40866, max_pubs_per_author=10000, concurrency=72)


if __name__ == "__main__":
    asyncio.run(main())

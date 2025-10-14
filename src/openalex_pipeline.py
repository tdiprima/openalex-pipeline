import asyncio
import os
from dataclasses import dataclass
from typing import List, Optional
from urllib.parse import quote_plus

import aiohttp
import asyncpg
from dotenv import load_dotenv

load_dotenv()


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
    STONYBROOK_ROR = "05qghxh33"

    def __init__(self, db_url: str, email: str):
        self.db_url = db_url
        self.email = email
        self.conn = None

    async def connect_db(self):
        """Connect to PostgreSQL"""
        self.conn = await asyncpg.connect(self.db_url, ssl=False)
        await self.conn.execute(
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
        await self.conn.execute(
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
    ):
        """Fetch authors from Stony Brook"""
        authors = []
        page = 1
        per_page = 200

        while len(authors) < max_results:
            url = f"{self.BASE_URL}/authors"
            params = {
                "filter": f"affiliations.institution.ror:{self.STONYBROOK_ROR}",
                "per-page": per_page,
                "page": page,
                "sort": "cited_by_count:desc",
                "mailto": self.email,
            }

            async with session.get(url, params=params) as resp:
                data = await resp.json()
                results = data.get("results", [])

                if not results:
                    break

                for item in results:
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

                print(f"  Fetched page {page}, total authors so far: {len(authors)}")

                if len(results) < per_page:
                    break

                page += 1
                await asyncio.sleep(0.1)

        return authors[:max_results]

    async def fetch_publications(
        self, session: aiohttp.ClientSession, author_id: str, max_results: int = 10000
    ):
        """Fetch publications for an author"""
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
                data = await resp.json()
                results = data.get("results", [])

                if not results:
                    break

                for item in results:
                    # Convert inverted index to text if present
                    abstract = None
                    if item.get("abstract_inverted_index"):
                        abstract = str(item.get("abstract_inverted_index"))[:5000]

                    pub = Publication(
                        id=item["id"][:500],
                        title=item.get("title", "")[:1000],
                        doi=item.get("doi", "")[:500] if item.get("doi") else None,
                        publication_year=item.get("publication_year", 0),
                        pdf_url=(
                            item.get("primary_location", {}).get("pdf_url", "")[:1000]
                            if item.get("primary_location")
                            and item.get("primary_location", {}).get("pdf_url")
                            else None
                        ),
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
        """Save author to database"""
        await self.conn.execute(
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
        """Save publication to database"""
        await self.conn.execute(
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

    async def run(self, max_authors: int = 10000, max_pubs_per_author: int = 10000):
        """Main pipeline"""
        await self.connect_db()

        async with aiohttp.ClientSession() as session:
            # Get authors
            print("Fetching authors...")
            authors = await self.fetch_authors(session, max_authors)
            print(f"Found {len(authors)} total authors")

            # Save authors and get their publications
            for i, author in enumerate(authors):
                print(f"Processing author {i+1}/{len(authors)}: {author.name}")
                await self.save_author(author)

                pubs = await self.fetch_publications(
                    session, author.id, max_pubs_per_author
                )
                print(f"  Found {len(pubs)} publications")

                for pub in pubs:
                    await self.save_publication(pub)

                await asyncio.sleep(0.1)  # Be nice to API

        await self.conn.close()
        print("Done!")


# Usage
async def main():
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST", "localhost")
    db_name = os.getenv("DB_NAME")
    email = os.getenv("OPENALEX_EMAIL")

    # URL-encode password to handle special characters
    db_url = f"postgresql://{db_user}:{quote_plus(db_password)}@{db_host}/{db_name}"
    pipeline = OpenAlexPipeline(db_url, email)
    await pipeline.run(max_authors=10000, max_pubs_per_author=10000)


if __name__ == "__main__":
    asyncio.run(main())

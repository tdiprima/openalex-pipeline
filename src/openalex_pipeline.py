import asyncio
import os
from dataclasses import dataclass
from typing import List, Optional

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
        self, session: aiohttp.ClientSession, max_results: int = 200
    ):
        """Fetch authors from Stony Brook"""
        url = f"{self.BASE_URL}/authors"
        params = {
            "filter": f"affiliations.institution.ror:{self.STONYBROOK_ROR}",
            "per-page": min(max_results, 200),
            "sort": "cited_by_count:desc",
            "mailto": self.email,
        }

        async with session.get(url, params=params) as resp:
            data = await resp.json()
            authors = []
            for item in data.get("results", []):
                author = Author(
                    id=item["id"],
                    name=item.get("display_name", ""),
                    works_count=item.get("works_count", 0),
                    cited_by_count=item.get("cited_by_count", 0),
                    affiliations=[
                        aff.get("display_name", "")
                        for aff in item.get("affiliations", [])
                    ],
                )
                authors.append(author)
            return authors

    async def fetch_publications(
        self, session: aiohttp.ClientSession, author_id: str, max_results: int = 200
    ):
        """Fetch publications for an author"""
        url = f"{self.BASE_URL}/works"
        params = {
            "filter": f"authorships.author.id:{author_id}",
            "per-page": min(max_results, 200),
            "sort": "publication_year:desc",
            "mailto": self.email,
        }

        async with session.get(url, params=params) as resp:
            data = await resp.json()
            pubs = []
            for item in data.get("results", []):
                pub = Publication(
                    id=item["id"],
                    title=item.get("title", ""),
                    doi=item.get("doi"),
                    publication_year=item.get("publication_year", 0),
                    pdf_url=(
                        item.get("primary_location", {}).get("pdf_url")
                        if item.get("primary_location")
                        else None
                    ),
                    authors=[
                        a.get("author", {}).get("display_name", "")
                        for a in item.get("authorships", [])
                    ],
                    abstract=item.get("abstract_inverted_index"),
                )
                pubs.append(pub)
            return pubs

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

    async def run(self, max_authors: int = 50, max_pubs_per_author: int = 100):
        """Main pipeline"""
        await self.connect_db()

        async with aiohttp.ClientSession() as session:
            # Get authors
            print("Fetching authors...")
            authors = await self.fetch_authors(session, max_authors)
            print(f"Found {len(authors)} authors")

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

    db_url = f"postgresql://{db_user}:{db_password}@{db_host}/{db_name}"
    pipeline = OpenAlexPipeline(db_url, email)
    await pipeline.run(max_authors=50, max_pubs_per_author=100)


if __name__ == "__main__":
    asyncio.run(main())

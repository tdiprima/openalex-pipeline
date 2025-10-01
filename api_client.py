"""
OpenAlex API client for querying authors and publications.
"""

from typing import List, Optional

import requests

from models import Author, Publication


class OpenAlexAPI:
    """Handles OpenAlex API interactions"""

    BASE_URL = "https://api.openalex.org"

    def __init__(self, email: Optional[str] = None):
        """
        Initialize API client.

        Args:
            email: Your email for polite pool (faster, recommended)
        """
        self.email = email
        self.session = requests.Session()
        if email:
            self.session.params = {"mailto": email}

    def find_stonybrook_authors(self, max_results: int = 25) -> List[Author]:
        """
        Find authors affiliated with Stony Brook University.

        Uses institution ROR ID for accurate matching.
        Stony Brook University ROR: https://ror.org/05qghxh33
        Stony Brook Medicine: https://ror.org/05wyq9e07
        State University of New York: https://ror.org/01q1z8k08
        """
        authors = []

        # Stony Brook University ROR ID (just the identifier, not full URL)
        stonybrook_ror = "05qghxh33"

        # Query for authors with Stony Brook affiliation
        url = f"{self.BASE_URL}/authors"
        params = {
            "filter": f"last_known_institutions.ror:{stonybrook_ror}",
            "per-page": min(max_results, 200),
            "sort": "cited_by_count:desc",  # Get most cited authors first
        }

        print("Querying OpenAlex for Stony Brook authors...")
        response = self.session.get(url, params=params)
        response.raise_for_status()

        data = response.json()

        for result in data.get("results", [])[:max_results]:
            affiliations = [
                inst.get("display_name", "")
                for inst in result.get("last_known_institutions", [])
            ]

            author = Author(
                id=result["id"],
                name=result.get("display_name", "Unknown"),
                works_count=result.get("works_count", 0),
                cited_by_count=result.get("cited_by_count", 0),
                affiliations=affiliations,
            )
            authors.append(author)
            print(f"  Found: {author.name} ({author.works_count} works)")

        return authors

    def get_author_publications(
        self, author_id: str, max_results: int = 10
    ) -> List[Publication]:
        """
        Get publications for a specific author.

        Args:
            author_id: OpenAlex author ID
            max_results: Maximum number of publications to retrieve
        """
        publications = []

        url = f"{self.BASE_URL}/works"
        params = {
            "filter": f"authorships.author.id:{author_id}",
            "per-page": min(max_results, 200),
            "sort": "publication_year:desc",
        }

        response = self.session.get(url, params=params)
        response.raise_for_status()

        data = response.json()

        for result in data.get("results", [])[:max_results]:
            # Extract PDF URL if available
            pdf_url = None
            if result.get("open_access", {}).get("is_oa"):
                pdf_url = result.get("open_access", {}).get("oa_url")

            # Extract author names
            author_names = [
                authorship.get("author", {}).get("display_name", "Unknown")
                for authorship in result.get("authorships", [])
            ]

            pub = Publication(
                id=result["id"],
                title=result.get("title", "Untitled"),
                doi=result.get("doi"),
                publication_year=result.get("publication_year", 0),
                pdf_url=pdf_url,
                authors=author_names[:5],  # Limit to first 5 authors
                abstract=result.get("abstract"),
            )
            publications.append(pub)

        return publications

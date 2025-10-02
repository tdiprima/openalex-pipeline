"""
Core query interface for chunked JSONL datasets.
"""

from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional

from file_io import read_jsonl_file


class DatasetQuery:
    """Query interface for chunked JSONL datasets"""

    def __init__(self, run_directory: str):
        """
        Initialize query interface for a specific run.

        Args:
            run_directory: Path to the run directory containing chunked files
        """
        self.run_dir = Path(run_directory)
        if not self.run_dir.exists():
            raise ValueError(f"Run directory does not exist: {run_directory}")

        # Discover available files
        self.authors_file = self._find_authors_file()
        self.publication_chunks = self._find_publication_chunks()

        print("Dataset Query initialized:")
        print(f"  Run directory: {self.run_dir}")
        print(
            f"  Authors file: {self.authors_file.name if self.authors_file else 'Not found'}"
        )
        print(f"  Publication chunks: {len(self.publication_chunks)}")

    def _find_authors_file(self) -> Optional[Path]:
        """Find the authors file (compressed or uncompressed)"""
        patterns = ["authors.jsonl.gz", "authors.jsonl"]
        for pattern in patterns:
            files = list(self.run_dir.glob(pattern))
            if files:
                return files[0]
        return None

    def _find_publication_chunks(self) -> List[Path]:
        """Find all publication chunk files"""
        patterns = [
            "publications_chunk_*.jsonl.gz",
            "publications_chunk_*.jsonl",
            "publications.jsonl.gz",
            "publications.jsonl",
        ]
        chunks = []
        for pattern in patterns:
            chunks.extend(self.run_dir.glob(pattern))
        return sorted(chunks)

    def query_authors(
        self, filter_func: Optional[Callable[[Dict], bool]] = None
    ) -> Iterator[Dict[str, Any]]:
        """
        Query authors with optional filtering.

        Args:
            filter_func: Optional function to filter authors

        Yields:
            Author records matching the filter
        """
        if not self.authors_file:
            print("No authors file found")
            return

        for record in read_jsonl_file(self.authors_file):
            if not filter_func or filter_func(record):
                yield record

    def query_publications(
        self, filter_func: Optional[Callable[[Dict], bool]] = None
    ) -> Iterator[Dict[str, Any]]:
        """
        Query publications across all chunks with optional filtering.

        Args:
            filter_func: Optional function to filter publications

        Yields:
            Publication records matching the filter
        """
        for chunk_file in self.publication_chunks:
            for record in read_jsonl_file(chunk_file):
                if not filter_func or filter_func(record):
                    yield record

    def get_author_by_id(self, author_id: str) -> Optional[Dict[str, Any]]:
        """Find a specific author by ID"""
        for author in self.query_authors(lambda a: a.get("id") == author_id):
            return author
        return None

    def get_author_by_name(
        self, name: str, exact: bool = False
    ) -> Iterator[Dict[str, Any]]:
        """
        Find authors by name.

        Args:
            name: Author name to search for
            exact: If True, require exact match; if False, case-insensitive substring match
        """
        if exact:

            def filter_func(a):
                return a.get("name") == name

        else:
            name_lower = name.lower()

            def filter_func(a):
                return name_lower in a.get("name", "").lower()

        for author in self.query_authors(filter_func):
            yield author

    def get_publications_by_author(self, author_id: str) -> Iterator[Dict[str, Any]]:
        """Get all publications for a specific author"""
        for pub in self.query_publications(lambda p: p.get("author_id") == author_id):
            yield pub

    def get_publications_by_year(self, year: int) -> Iterator[Dict[str, Any]]:
        """Get all publications from a specific year"""
        for pub in self.query_publications(lambda p: p.get("publication_year") == year):
            yield pub

    def get_publications_by_year_range(
        self, start_year: int, end_year: int
    ) -> Iterator[Dict[str, Any]]:
        """Get all publications within a year range (inclusive)"""

        def year_filter(pub):
            year = pub.get("publication_year")
            return year and start_year <= year <= end_year

        for pub in self.query_publications(year_filter):
            yield pub

    def search_publications_by_title(
        self, title_query: str, case_sensitive: bool = False
    ) -> Iterator[Dict[str, Any]]:
        """
        Search publications by title text.

        Args:
            title_query: Text to search for in titles
            case_sensitive: Whether search should be case sensitive
        """
        if case_sensitive:

            def filter_func(p):
                return title_query in p.get("title", "")

        else:
            query_lower = title_query.lower()

            def filter_func(p):
                return query_lower in p.get("title", "").lower()

        for pub in self.query_publications(filter_func):
            yield pub

    def get_publications_with_pdfs(self) -> Iterator[Dict[str, Any]]:
        """Get all publications that have PDF downloads"""

        def filter_func(p):
            return p.get("processing", {}).get("pdf_downloaded", False)

        for pub in self.query_publications(filter_func):
            yield pub

    def get_publications_with_stonybrook_mentions(self) -> Iterator[Dict[str, Any]]:
        """Get all publications with Stony Brook mentions in content"""

        def filter_func(p):
            return (
                p.get("processing", {})
                .get("stonybrook_validation", {})
                .get("found", False)
            )

        for pub in self.query_publications(filter_func):
            yield pub

    def get_authors_with_stonybrook_mentions(self) -> Iterator[Dict[str, Any]]:
        """
        Get all authors who have publications with Stony Brook mentions.

        Yields:
            Author records with Stony Brook-related publications
        """
        # Collect unique author IDs from Stony Brook publications
        stonybrook_author_ids = set()
        for pub in self.get_publications_with_stonybrook_mentions():
            author_id = pub.get("author_id")
            if author_id:
                stonybrook_author_ids.add(author_id)

        # Yield author records
        for author_id in stonybrook_author_ids:
            author = self.get_author_by_id(author_id)
            if author:
                yield author

    def count_authors(
        self, filter_func: Optional[Callable[[Dict], bool]] = None
    ) -> int:
        """Count authors matching filter"""
        return sum(1 for _ in self.query_authors(filter_func))

    def count_publications(
        self, filter_func: Optional[Callable[[Dict], bool]] = None
    ) -> int:
        """Count publications matching filter"""
        return sum(1 for _ in self.query_publications(filter_func))

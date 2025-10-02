"""
Statistics and aggregation functions for datasets.
"""

from operator import itemgetter
from typing import Any, Dict


def get_statistics(query) -> Dict[str, Any]:
    """
    Get comprehensive dataset statistics.

    Args:
        query: DatasetQuery instance

    Returns:
        Dictionary containing various statistics
    """
    stats = {
        "total_authors": query.count_authors(),
        "total_publications": query.count_publications(),
        "publications_with_pdfs": query.count_publications(
            lambda p: p.get("processing", {}).get("pdf_downloaded", False)
        ),
        "publications_with_stonybrook_mentions": query.count_publications(
            lambda p: p.get("processing", {})
            .get("stonybrook_validation", {})
            .get("found", False)
        ),
        "authors_with_stonybrook_mentions": sum(
            1 for _ in query.get_authors_with_stonybrook_mentions()
        ),
        "unique_years": set(),
        "top_authors_by_publications": [],
        "run_directory": str(query.run_dir),
    }

    # Collect year distribution and author publication counts
    years = set()
    author_pub_counts = {}

    for pub in query.query_publications():
        year = pub.get("publication_year")
        if year:
            years.add(year)

        author_id = pub.get("author_id")
        if author_id:
            author_pub_counts[author_id] = author_pub_counts.get(author_id, 0) + 1

    stats["unique_years"] = sorted(years)
    stats["year_range"] = f"{min(years)}-{max(years)}" if years else "N/A"

    # Top authors by publication count
    top_authors = sorted(author_pub_counts.items(), key=itemgetter(1), reverse=True)[
        :10
    ]

    for author_id, count in top_authors:
        author = query.get_author_by_id(author_id)
        if author:
            stats["top_authors_by_publications"].append(
                {
                    "author_id": author_id,
                    "name": author.get("name"),
                    "publication_count": count,
                }
            )

    return stats

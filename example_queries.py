#!/usr/bin/env python3
"""
Example queries for analyzing JSONL export data from the OpenAlex pipeline.
Shows how to connect authors.jsonl and publications.jsonl files.
"""

import json
import operator
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List


def load_jsonl(file_path: str) -> List[Dict[str, Any]]:
    """Load JSONL file into a list of dictionaries"""
    data = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            data.append(json.loads(line.strip()))
    return data


def load_authors_index(authors_file: str) -> Dict[str, Dict[str, Any]]:
    """Load authors into a dictionary indexed by author ID"""
    authors = {}
    with open(authors_file, "r", encoding="utf-8") as f:
        for line in f:
            author = json.loads(line.strip())
            authors[author["id"]] = author
    return authors


def example_queries(run_directory: str):
    """Demonstrate various queries across the JSONL files"""

    authors_file = f"{run_directory}/authors.jsonl"
    publications_file = f"{run_directory}/publications.jsonl"

    print("=" * 60)
    print("JSONL QUERY EXAMPLES")
    print("=" * 60)

    # Load data
    print("\n1. LOADING DATA")
    print("-" * 30)
    authors = load_authors_index(authors_file)
    publications = load_jsonl(publications_file)

    print(f"Loaded {len(authors)} authors and {len(publications)} publications")

    # Query 1: Find all publications by a specific author
    print("\n2. PUBLICATIONS BY SPECIFIC AUTHOR")
    print("-" * 30)
    sample_author_id = next(iter(authors.keys()))  # Get first author ID
    author_name = authors[sample_author_id]["name"]

    author_pubs = [pub for pub in publications if pub["author_id"] == sample_author_id]
    print(f"Author: {author_name}")
    print(f"Publications found: {len(author_pubs)}")
    for pub in author_pubs:
        print(f"  - {pub['title'][:60]}... ({pub['publication_year']})")

    # Query 2: Find authors with Stony Brook mentions
    print("\n3. AUTHORS WITH STONY BROOK MENTIONS")
    print("-" * 30)
    sb_authors = [
        author for author in authors.values() if author["stonybrook_mentions"] > 0
    ]
    print(f"Authors with Stony Brook mentions: {len(sb_authors)}")
    for author in sb_authors[:5]:  # Show first 5
        print(f"  - {author['name']}: {author['stonybrook_mentions']} mentions")

    # Query 3: Publication statistics by year
    print("\n4. PUBLICATIONS BY YEAR")
    print("-" * 30)
    year_stats = defaultdict(int)
    for pub in publications:
        if pub["publication_year"]:
            year_stats[pub["publication_year"]] += 1

    sorted_years = sorted(year_stats.items(), reverse=True)
    for year, count in sorted_years[:10]:  # Show top 10 years
        print(f"  {year}: {count} publications")

    # Query 4: PDF download success rate
    print("\n5. PDF PROCESSING STATISTICS")
    print("-" * 30)
    total_pubs = len(publications)
    pdfs_downloaded = sum(
        1 for pub in publications if pub["processing"]["pdf_downloaded"]
    )
    texts_extracted = sum(
        1 for pub in publications if pub["processing"]["text_extracted"]
    )
    summaries_generated = sum(1 for pub in publications if pub["processing"]["summary"])

    print(f"Total publications: {total_pubs}")
    print(f"PDFs downloaded: {pdfs_downloaded} ({pdfs_downloaded/total_pubs*100:.1f}%)")
    print(f"Texts extracted: {texts_extracted} ({texts_extracted/total_pubs*100:.1f}%)")
    print(
        f"Summaries generated: {summaries_generated} ({summaries_generated/total_pubs*100:.1f}%)"
    )

    # Query 5: Most productive authors
    print("\n6. MOST PRODUCTIVE AUTHORS (by citations)")
    print("-" * 30)
    sorted_authors = sorted(
        authors.values(), key=operator.itemgetter("cited_by_count"), reverse=True
    )
    for author in sorted_authors[:10]:  # Top 10
        print(
            f"  {author['name']}: {author['cited_by_count']} citations, {author['publications_processed']} pubs processed"
        )

    # Query 6: Join example - Author details with their publications
    print("\n7. DETAILED AUTHOR-PUBLICATION JOIN")
    print("-" * 30)
    sample_author = sorted_authors[0]  # Take most cited author
    sample_author_pubs = [
        pub for pub in publications if pub["author_id"] == sample_author["id"]
    ]

    print(f"Author: {sample_author['name']}")
    print(f"Total works: {sample_author['works_count']}")
    print(f"Citations: {sample_author['cited_by_count']}")
    print(f"Affiliations: {', '.join(sample_author['affiliations'])}")
    print(f"Publications processed: {len(sample_author_pubs)}")
    print("\nPublications:")
    for pub in sample_author_pubs:
        status = "✓ PDF" if pub["processing"]["pdf_downloaded"] else "✗ No PDF"
        sb_status = (
            f"SB: {pub['processing']['stonybrook_validation'].get('count', 0)}"
            if pub["processing"]["stonybrook_validation"]
            else "SB: 0"
        )
        print(
            f"  - {pub['title'][:50]}... ({pub['publication_year']}) [{status}] [{sb_status}]"
        )


def jq_examples():
    """Show jq command examples for querying JSONL files"""
    print("\n" + "=" * 60)
    print("JQ COMMAND LINE EXAMPLES")
    print("=" * 60)

    examples = [
        ("Count total authors", "wc -l authors.jsonl"),
        ("Count total publications", "wc -l publications.jsonl"),
        ("Get author names only", "jq -r '.name' authors.jsonl"),
        (
            "Find authors with >1000 citations",
            "jq 'select(.cited_by_count > 1000)' authors.jsonl",
        ),
        (
            "Get publications from 2023",
            "jq 'select(.publication_year == 2023)' publications.jsonl",
        ),
        (
            "Count PDFs downloaded",
            "jq 'select(.processing.pdf_downloaded == true)' publications.jsonl | wc -l",
        ),
        (
            "Find publications with Stony Brook mentions",
            "jq 'select(.processing.stonybrook_validation.found == true)' publications.jsonl",
        ),
        ("Get publication titles only", "jq -r '.title' publications.jsonl"),
        (
            "Join: Get author name for each publication",
            "jq -r '[.author_name, .title] | @csv' publications.jsonl",
        ),
    ]

    for description, command in examples:
        print(f"\n{description}:")
        print(f"  {command}")


if __name__ == "__main__":
    # Example usage
    import sys

    if len(sys.argv) != 2:
        print("Usage: python example_queries.py <run_directory>")
        print("Example: python example_queries.py ./output/run_20231001_183045")
        sys.exit(1)

    run_dir = sys.argv[1]
    if not Path(run_dir).exists():
        print(f"Directory not found: {run_dir}")
        sys.exit(1)

    example_queries(run_dir)
    jq_examples()

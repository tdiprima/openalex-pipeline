"""
Command-line interface for querying datasets.

Usage examples:
  python cli.py --list-runs
  python cli.py --run-dir ./output/run_20231002_143022 --stats
  python cli.py --run-dir ./output/run_20231002_143022 --author "Einstein"
  python cli.py --run-dir ./output/run_20231002_143022 --stonybrook-authors
  python cli.py --run-dir ./output/run_20231002_143022 --combine
"""

import argparse
from statistics import get_statistics

from dataset_query import DatasetQuery
from run_manager import RunManager
from utils import combine_all_publications


def main():
    parser = argparse.ArgumentParser(description="Query chunked dataset")
    parser.add_argument("--run-dir", type=str, help="Run directory to query")
    parser.add_argument("--list-runs", action="store_true", help="List available runs")
    parser.add_argument("--stats", action="store_true", help="Show dataset statistics")
    parser.add_argument("--author", type=str, help="Search for author by name")
    parser.add_argument("--year", type=int, help="Find publications by year")
    parser.add_argument(
        "--stonybrook-authors",
        action="store_true",
        help="List all authors with Stony Brook mentions",
    )
    parser.add_argument(
        "--combine", action="store_true", help="Combine all chunks into single file"
    )

    args = parser.parse_args()

    if args.list_runs:
        manager = RunManager()
        runs = manager.list_runs()
        print(f"Found {len(runs)} runs:")
        for run in runs:
            print(
                f"  {run['run_name']}: {run['total_authors']} authors, "
                f"{run['total_publications']} publications"
            )

    elif args.run_dir:
        query = DatasetQuery(args.run_dir)

        if args.stats:
            stats = get_statistics(query)
            print("\nDataset Statistics:")
            for key, value in stats.items():
                if key != "top_authors_by_publications":
                    print(f"  {key}: {value}")

            if stats["top_authors_by_publications"]:
                print("\n  Top Authors:")
                for author in stats["top_authors_by_publications"]:
                    print(
                        f"    - {author['name']}: {author['publication_count']} publications"
                    )

        if args.author:
            print(f"\nSearching for author: {args.author}")
            count = 0
            for author in query.get_author_by_name(args.author):
                print(
                    f"  {author['name']} (ID: {author.get('id')}) - "
                    f"{author.get('works_count', 'N/A')} works"
                )
                count += 1
            print(f"Found {count} matching authors")

        if args.stonybrook_authors:
            print("\nAuthors with Stony Brook mentions:")
            count = 0
            for author in query.get_authors_with_stonybrook_mentions():
                print(
                    f"  {author['name']} (ID: {author.get('id')}) - "
                    f"{author.get('works_count', 'N/A')} works"
                )
                count += 1
            print(f"\nTotal: {count} authors with Stony Brook mentions")

        if args.year:
            print(f"\nPublications from {args.year}:")
            count = 0
            for pub in query.get_publications_by_year(args.year):
                print(f"  {pub['title'][:100]}...")
                count += 1
                if count >= 10:  # Limit output
                    print("  ... (showing first 10 of many)")
                    break

        if args.combine:
            combine_all_publications(args.run_dir)

    else:
        print(
            "Use --list-runs to see available runs or --run-dir to query a specific run"
        )
        print("Run with --help for more options")


if __name__ == "__main__":
    main()

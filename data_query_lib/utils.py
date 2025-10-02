"""
Utility and convenience functions.
"""

from pathlib import Path
from typing import Optional

from dataset_query import DatasetQuery
from file_io import write_jsonl_file
from run_manager import RunManager


def query_latest_run(output_dir: str = "../output") -> Optional[DatasetQuery]:
    """
    Quick access to the latest run.

    Args:
        output_dir: Base output directory

    Returns:
        DatasetQuery instance for the latest run, or None if no runs found
    """
    return RunManager(output_dir).get_latest_run()


def combine_all_publications(
    run_directory: str, output_file: str = "all_publications.jsonl"
):
    """
    Combine all publication chunks into a single file.

    Args:
        run_directory: Run directory containing chunked files
        output_file: Output filename
    """
    query = DatasetQuery(run_directory)
    output_path = Path(output_file)

    count = write_jsonl_file(output_path, query.query_publications(), compress=False)

    print(f"Combined {count} publications into {output_path}")


def export_filtered_data(
    query: DatasetQuery,
    output_file: str,
    publication_filter=None,
    compress: bool = True,
):
    """
    Export filtered publications to a new file.

    Args:
        query: DatasetQuery instance
        output_file: Output file path
        publication_filter: Filter function for publications
        compress: Whether to compress output
    """
    output_path = Path(output_file)

    count = write_jsonl_file(
        output_path, query.query_publications(publication_filter), compress=compress
    )

    print(f"Exported {count} publications to {output_path}")

"""
Data export module for saving pipeline results to structured formats.
"""

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

# JSON handling with fallback
try:
    import orjson

    JSON_LIB = "orjson"
except ImportError:
    import json

    JSON_LIB = "json"
    print("orjson not available, using standard json (install: pip install orjson)")

from models import Author, Publication


class DataExporter:
    """Handles exporting pipeline results to various formats"""

    def __init__(self, output_dir: str = "./output"):
        """
        Initialize exporter.

        Args:
            output_dir: Directory to save exported files
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

        # Create timestamped run directory
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.run_dir = self.output_dir / f"run_{timestamp}"
        self.run_dir.mkdir(exist_ok=True)

        # Initialize data storage
        self.results = {
            "metadata": {
                "timestamp": datetime.now().isoformat(),
                "json_library": JSON_LIB,
                "pipeline_version": "1.0",
            },
            "authors": [],
            "publications": [],
            "processing_stats": {
                "total_authors": 0,
                "total_publications": 0,
                "pdfs_downloaded": 0,
                "stonybrook_mentions_found": 0,
                "summaries_generated": 0,
            },
        }

    def add_author(
        self,
        author: Author,
        publications: List[Publication],
        processing_results: List[Dict[str, Any]],
    ):
        """
        Add author and their publications to the export data.

        Args:
            author: Author object
            publications: List of Publication objects
            processing_results: List of processing results for each publication
        """
        author_data = {
            "id": author.id,
            "name": author.name,
            "works_count": author.works_count,
            "cited_by_count": author.cited_by_count,
            "affiliations": author.affiliations,
            "publications": [],
        }

        for pub, result in zip(publications, processing_results):
            pub_data = {
                "id": pub.id,
                "title": pub.title,
                "doi": pub.doi,
                "publication_year": pub.publication_year,
                "authors": pub.authors,
                "abstract": pub.abstract,
                "pdf_url": pub.pdf_url,
                "processing": {
                    "pdf_downloaded": result.get("pdf_downloaded", False),
                    "pdf_path": result.get("pdf_path"),
                    "text_extracted": result.get("text_extracted", False),
                    "text_length": result.get("text_length", 0),
                    "stonybrook_validation": result.get("stonybrook_validation", {}),
                    "summary": result.get("summary"),
                    "processing_timestamp": result.get("timestamp"),
                },
            }

            author_data["publications"].append(pub_data)
            self.results["publications"].append(pub_data)

            # Update stats
            if result.get("pdf_downloaded"):
                self.results["processing_stats"]["pdfs_downloaded"] += 1
            if result.get("stonybrook_validation", {}).get("found"):
                self.results["processing_stats"]["stonybrook_mentions_found"] += 1
            if result.get("summary"):
                self.results["processing_stats"]["summaries_generated"] += 1

        self.results["authors"].append(author_data)
        self.results["processing_stats"]["total_authors"] += 1
        self.results["processing_stats"]["total_publications"] += len(publications)

    def save_json(self, filename: str = "pipeline_results.json") -> str:
        """
        Save results to JSON file.

        Args:
            filename: Name of the JSON file

        Returns:
            Path to the saved file
        """
        file_path = self.run_dir / filename

        if JSON_LIB == "orjson":
            # orjson produces bytes, need to write in binary mode
            with open(file_path, "wb") as f:
                f.write(
                    orjson.dumps(
                        self.results, option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS
                    )
                )
        else:
            # Standard json
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(
                    self.results,
                    f,
                    indent=2,
                    ensure_ascii=False,
                    default=str,
                    sort_keys=True,
                )

        return str(file_path)

    def save_authors_summary(self, filename: str = "authors_summary.json") -> str:
        """
        Save just authors summary (without full publication details).

        Args:
            filename: Name of the JSON file

        Returns:
            Path to the saved file
        """
        authors_summary = {
            "metadata": self.results["metadata"],
            "stats": self.results["processing_stats"],
            "authors": [
                {
                    "id": author["id"],
                    "name": author["name"],
                    "works_count": author["works_count"],
                    "cited_by_count": author["cited_by_count"],
                    "affiliations": author["affiliations"],
                    "publications_processed": len(author["publications"]),
                    "pdfs_found": sum(
                        1
                        for pub in author["publications"]
                        if pub["processing"]["pdf_downloaded"]
                    ),
                    "stonybrook_mentions": sum(
                        1
                        for pub in author["publications"]
                        if pub["processing"]["stonybrook_validation"].get(
                            "found", False
                        )
                    ),
                }
                for author in self.results["authors"]
            ],
        }

        file_path = self.run_dir / filename

        if JSON_LIB == "orjson":
            with open(file_path, "wb") as f:
                f.write(
                    orjson.dumps(
                        authors_summary,
                        option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS,
                    )
                )
        else:
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(
                    authors_summary,
                    f,
                    indent=2,
                    ensure_ascii=False,
                    default=str,
                    sort_keys=True,
                )

        return str(file_path)

    def get_run_directory(self) -> str:
        """Get the path to the current run directory."""
        return str(self.run_dir)

    def get_stats(self) -> Dict[str, Any]:
        """Get current processing statistics."""
        return self.results["processing_stats"].copy()

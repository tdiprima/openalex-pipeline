"""
Memory-efficient data export module for saving pipeline results using streaming writes.
Designed to handle large datasets (40k+ authors) without memory issues.
"""

import gzip
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


class StreamingDataExporter:
    """Memory-efficient exporter using JSONL streaming writes"""

    def __init__(self, output_dir: str = "./output", compress: bool = True, chunk_size: int = 1000):
        """
        Initialize streaming exporter.

        Args:
            output_dir: Directory to save exported files
            compress: Whether to compress output files (default: True)
            chunk_size: Number of records per chunk for large datasets (default: 1000)
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.compress = compress
        self.chunk_size = chunk_size
        self.current_chunk = 0
        self.records_in_chunk = 0

        # Create timestamped run directory
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.run_dir = self.output_dir / f"run_{timestamp}"
        self.run_dir.mkdir(exist_ok=True)

        # Lightweight metadata (no bulk data storage)
        self.metadata = {
            "timestamp": datetime.now().isoformat(),
            "json_library": JSON_LIB,
            "pipeline_version": "1.0",
            "format": "JSONL (JSON Lines)",
        }

        self.processing_stats = {
            "total_authors": 0,
            "total_publications": 0,
            "pdfs_downloaded": 0,
            "stonybrook_mentions_found": 0,
            "summaries_generated": 0,
            "start_time": datetime.now().isoformat(),
        }

        # Initialize streaming files with compression support
        ext = ".jsonl.gz" if self.compress else ".jsonl"
        self.authors_file_path = self.run_dir / f"authors{ext}"
        self.publications_file_path = self.run_dir / f"publications_chunk_{self.current_chunk:04d}{ext}"
        self.stats_file_path = self.run_dir / "run_stats.json"
        
        # Track all publication files for chunking
        self.publication_files = []

        self.authors_file = None
        self.publications_file = None
        self._initialize_files()

    def _initialize_files(self):
        """Initialize JSONL files for streaming writes with compression support"""
        try:
            if self.compress:
                self.authors_file = gzip.open(self.authors_file_path, "wt", encoding="utf-8")
                self.publications_file = gzip.open(self.publications_file_path, "wt", encoding="utf-8")
            else:
                self.authors_file = self.authors_file_path.open("w", encoding="utf-8")
                self.publications_file = self.publications_file_path.open("w", encoding="utf-8")
            
            self.publication_files.append(self.publications_file_path)
            compression_status = "compressed" if self.compress else "uncompressed"
            print(f"Initialized streaming export to: {self.run_dir} ({compression_status})")
        except Exception as e:
            print(f"Error initializing export files: {e}")
            raise

    def _write_json_line(self, file_handle, data):
        """Write a single JSON line to file (JSONL format)"""
        try:
            if JSON_LIB == "orjson":
                line = orjson.dumps(data).decode("utf-8")
            else:
                line = json.dumps(data, ensure_ascii=False, default=str)
            file_handle.write(line + "\n")
            file_handle.flush()  # Ensure immediate write to disk
        except Exception as e:
            print(f"Error writing JSON line: {e}")

    def add_author(
        self,
        author: Author,
        publications: List[Publication],
        processing_results: List[Dict[str, Any]],
    ):
        """
        Stream author and publication data directly to files (memory efficient).

        Args:
            author: Author object
            publications: List of Publication objects
            processing_results: List of processing results for each publication
        """
        # Author summary record (lightweight)
        author_record = {
            "id": author.id,
            "name": author.name,
            "works_count": author.works_count,
            "cited_by_count": author.cited_by_count,
            "affiliations": author.affiliations,
            "publications_processed": len(publications),
            "processing_timestamp": datetime.now().isoformat(),
        }

        # Process publications and collect summary stats
        pdfs_found = 0
        stonybrook_mentions = 0
        summaries_generated = 0

        for pub_idx, (pub, result) in enumerate(
            zip(publications, processing_results), 1
        ):
            # Create detailed publication record
            pub_record = {
                "id": pub.id,
                "title": pub.title,
                "doi": pub.doi,
                "publication_year": pub.publication_year,
                "authors": pub.authors,
                "abstract": pub.abstract,
                "pdf_url": pub.pdf_url,
                # Link back to author
                "author_id": author.id,
                "author_name": author.name,
                "sequence_number": pub_idx,
                # Processing results
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

            # Check if we need to start a new chunk
            if self.records_in_chunk >= self.chunk_size:
                self._rotate_publications_file()
            
            # Write publication record immediately (streaming)
            self._write_json_line(self.publications_file, pub_record)
            self.records_in_chunk += 1

            # Update statistics
            if result.get("pdf_downloaded"):
                pdfs_found += 1
                self.processing_stats["pdfs_downloaded"] += 1
            if result.get("stonybrook_validation", {}).get("found"):
                stonybrook_mentions += 1
                self.processing_stats["stonybrook_mentions_found"] += 1
            if result.get("summary"):
                summaries_generated += 1
                self.processing_stats["summaries_generated"] += 1

        # Add summary stats to author record
        author_record.update(
            {
                "pdfs_found": pdfs_found,
                "stonybrook_mentions": stonybrook_mentions,
                "summaries_generated": summaries_generated,
            }
        )

        # Write author record immediately (streaming)
        self._write_json_line(self.authors_file, author_record)

        # Update global stats
        self.processing_stats["total_authors"] += 1
        self.processing_stats["total_publications"] += len(publications)

    def save_stats(self) -> str:
        """
        Save final processing statistics.

        Returns:
            Path to the stats file
        """
        self.processing_stats["end_time"] = datetime.now().isoformat()

        final_stats = {
            "metadata": self.metadata,
            "processing_stats": self.processing_stats,
            "files": {
                "authors": str(self.authors_file_path),
                "publications_chunks": [str(f) for f in self.publication_files],
                "total_chunks": len(self.publication_files),
                "compression_enabled": self.compress,
                "chunk_size": self.chunk_size,
                "total_size_mb": self._calculate_total_size(),
            },
        }

        if JSON_LIB == "orjson":
            self.stats_file_path.write_bytes(
                orjson.dumps(final_stats, option=orjson.OPT_INDENT_2)
            )
        else:
            with self.stats_file_path.open("w", encoding="utf-8") as f:
                json.dump(final_stats, f, indent=2, ensure_ascii=False, default=str)

        return str(self.stats_file_path)

    def _rotate_publications_file(self):
        """Rotate to a new publications file chunk"""
        if self.publications_file:
            self.publications_file.close()
        
        self.current_chunk += 1
        self.records_in_chunk = 0
        
        ext = ".jsonl.gz" if self.compress else ".jsonl"
        self.publications_file_path = self.run_dir / f"publications_chunk_{self.current_chunk:04d}{ext}"
        
        if self.compress:
            self.publications_file = gzip.open(self.publications_file_path, "wt", encoding="utf-8")
        else:
            self.publications_file = self.publications_file_path.open("w", encoding="utf-8")
        
        self.publication_files.append(self.publications_file_path)
        print(f"Rotated to new publications chunk: {self.publications_file_path.name}")

    def _calculate_total_size(self) -> float:
        """Calculate total size of output files in MB"""
        total_size = 0
        # Include authors file
        if self.authors_file_path.exists():
            total_size += self.authors_file_path.stat().st_size
        
        # Include all publication chunk files
        for pub_file in self.publication_files:
            if pub_file.exists():
                total_size += pub_file.stat().st_size
        
        return round(total_size / (1024 * 1024), 2)  # Convert to MB

    def close(self):
        """Close file handles and finalize export"""
        try:
            if self.authors_file:
                self.authors_file.close()
            if self.publications_file:
                self.publications_file.close()
            print(f"Export completed. Files saved to: {self.run_dir}")
        except Exception as e:
            print(f"Error closing export files: {e}")

    def get_run_directory(self) -> str:
        """Get the path to the current run directory."""
        return str(self.run_dir)

    def get_stats(self) -> Dict[str, Any]:
        """Get current processing statistics."""
        return self.processing_stats.copy()

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures files are closed"""
        self.close()


# For backward compatibility, create an alias
DataExporter = StreamingDataExporter

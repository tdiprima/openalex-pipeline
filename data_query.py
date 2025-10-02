"""
Query utilities for chunked and compressed JSONL data.
Handles transparent querying across multiple compressed chunk files.

# List all runs
python data_query.py --list-runs

# Get statistics
python data_query.py --run-dir ./output/run_20231002_143022 --stats

# Search for authors
python data_query.py --run-dir ./output/run_20231002_143022 --author "Einstein"

# Combine chunks into single file
python data_query.py --run-dir ./output/run_20231002_143022 --combine
"""

import gzip
import json
import glob
from pathlib import Path
from typing import Iterator, Dict, Any, List, Optional, Callable
from datetime import datetime
import re

# JSON handling with fallback
try:
    import orjson
    JSON_LIB = "orjson"
except ImportError:
    JSON_LIB = "json"


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
        
        print(f"Dataset Query initialized:")
        print(f"  Run directory: {self.run_dir}")
        print(f"  Authors file: {self.authors_file.name if self.authors_file else 'Not found'}")
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
        patterns = ["publications_chunk_*.jsonl.gz", "publications_chunk_*.jsonl", "publications.jsonl.gz", "publications.jsonl"]
        chunks = []
        for pattern in patterns:
            chunks.extend(self.run_dir.glob(pattern))
        return sorted(chunks)
    
    def _read_jsonl_file(self, file_path: Path) -> Iterator[Dict[str, Any]]:
        """Read a JSONL file (compressed or uncompressed)"""
        if file_path.suffix == '.gz':
            file_handle = gzip.open(file_path, 'rt', encoding='utf-8')
        else:
            file_handle = file_path.open('r', encoding='utf-8')
        
        try:
            for line in file_handle:
                line = line.strip()
                if not line:
                    continue
                
                if JSON_LIB == "orjson":
                    record = orjson.loads(line)
                else:
                    record = json.loads(line)
                yield record
        finally:
            file_handle.close()
    
    def query_authors(self, filter_func: Optional[Callable[[Dict], bool]] = None) -> Iterator[Dict[str, Any]]:
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
        
        for record in self._read_jsonl_file(self.authors_file):
            if not filter_func or filter_func(record):
                yield record
    
    def query_publications(self, filter_func: Optional[Callable[[Dict], bool]] = None) -> Iterator[Dict[str, Any]]:
        """
        Query publications across all chunks with optional filtering.
        
        Args:
            filter_func: Optional function to filter publications
            
        Yields:
            Publication records matching the filter
        """
        for chunk_file in self.publication_chunks:
            for record in self._read_jsonl_file(chunk_file):
                if not filter_func or filter_func(record):
                    yield record
    
    def get_author_by_id(self, author_id: str) -> Optional[Dict[str, Any]]:
        """Find a specific author by ID"""
        for author in self.query_authors(lambda a: a.get('id') == author_id):
            return author
        return None
    
    def get_author_by_name(self, name: str, exact: bool = False) -> Iterator[Dict[str, Any]]:
        """
        Find authors by name.
        
        Args:
            name: Author name to search for
            exact: If True, require exact match; if False, case-insensitive substring match
        """
        if exact:
            filter_func = lambda a: a.get('name') == name
        else:
            name_lower = name.lower()
            filter_func = lambda a: name_lower in a.get('name', '').lower()
        
        for author in self.query_authors(filter_func):
            yield author
    
    def get_publications_by_author(self, author_id: str) -> Iterator[Dict[str, Any]]:
        """Get all publications for a specific author"""
        for pub in self.query_publications(lambda p: p.get('author_id') == author_id):
            yield pub
    
    def get_publications_by_year(self, year: int) -> Iterator[Dict[str, Any]]:
        """Get all publications from a specific year"""
        for pub in self.query_publications(lambda p: p.get('publication_year') == year):
            yield pub
    
    def get_publications_by_year_range(self, start_year: int, end_year: int) -> Iterator[Dict[str, Any]]:
        """Get all publications within a year range (inclusive)"""
        def year_filter(pub):
            year = pub.get('publication_year')
            return year and start_year <= year <= end_year
        
        for pub in self.query_publications(year_filter):
            yield pub
    
    def search_publications_by_title(self, title_query: str, case_sensitive: bool = False) -> Iterator[Dict[str, Any]]:
        """
        Search publications by title text.
        
        Args:
            title_query: Text to search for in titles
            case_sensitive: Whether search should be case sensitive
        """
        if case_sensitive:
            filter_func = lambda p: title_query in p.get('title', '')
        else:
            query_lower = title_query.lower()
            filter_func = lambda p: query_lower in p.get('title', '').lower()
        
        for pub in self.query_publications(filter_func):
            yield pub
    
    def get_publications_with_pdfs(self) -> Iterator[Dict[str, Any]]:
        """Get all publications that have PDF downloads"""
        filter_func = lambda p: p.get('processing', {}).get('pdf_downloaded', False)
        for pub in self.query_publications(filter_func):
            yield pub
    
    def get_publications_with_stonybrook_mentions(self) -> Iterator[Dict[str, Any]]:
        """Get all publications with Stony Brook mentions in content"""
        filter_func = lambda p: p.get('processing', {}).get('stonybrook_validation', {}).get('found', False)
        for pub in self.query_publications(filter_func):
            yield pub
    
    def count_authors(self, filter_func: Optional[Callable[[Dict], bool]] = None) -> int:
        """Count authors matching filter"""
        return sum(1 for _ in self.query_authors(filter_func))
    
    def count_publications(self, filter_func: Optional[Callable[[Dict], bool]] = None) -> int:
        """Count publications matching filter"""
        return sum(1 for _ in self.query_publications(filter_func))
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get comprehensive dataset statistics"""
        stats = {
            'total_authors': self.count_authors(),
            'total_publications': self.count_publications(),
            'publications_with_pdfs': self.count_publications(
                lambda p: p.get('processing', {}).get('pdf_downloaded', False)
            ),
            'publications_with_stonybrook_mentions': self.count_publications(
                lambda p: p.get('processing', {}).get('stonybrook_validation', {}).get('found', False)
            ),
            'unique_years': set(),
            'top_authors_by_publications': [],
            'run_directory': str(self.run_dir)
        }
        
        # Collect year distribution
        years = set()
        author_pub_counts = {}
        
        for pub in self.query_publications():
            year = pub.get('publication_year')
            if year:
                years.add(year)
            
            author_id = pub.get('author_id')
            if author_id:
                author_pub_counts[author_id] = author_pub_counts.get(author_id, 0) + 1
        
        stats['unique_years'] = sorted(years)
        stats['year_range'] = f"{min(years)}-{max(years)}" if years else "N/A"
        
        # Top authors by publication count
        top_authors = sorted(author_pub_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        for author_id, count in top_authors:
            author = self.get_author_by_id(author_id)
            if author:
                stats['top_authors_by_publications'].append({
                    'author_id': author_id,
                    'name': author.get('name'),
                    'publication_count': count
                })
        
        return stats
    
    def export_filtered_data(self, output_file: str, publication_filter: Optional[Callable] = None, 
                           author_filter: Optional[Callable] = None, compress: bool = True):
        """
        Export filtered data to a new file.
        
        Args:
            output_file: Output file path
            publication_filter: Filter function for publications
            author_filter: Filter function for authors
            compress: Whether to compress output
        """
        output_path = Path(output_file)
        
        if compress and not output_path.suffix == '.gz':
            output_path = output_path.with_suffix(output_path.suffix + '.gz')
        
        if compress:
            file_handle = gzip.open(output_path, 'wt', encoding='utf-8')
        else:
            file_handle = output_path.open('w', encoding='utf-8')
        
        try:
            # Export filtered publications
            count = 0
            for pub in self.query_publications(publication_filter):
                if JSON_LIB == "orjson":
                    line = orjson.dumps(pub).decode('utf-8')
                else:
                    line = json.dumps(pub, ensure_ascii=False)
                file_handle.write(line + '\n')
                count += 1
            
            print(f"Exported {count} publications to {output_path}")
            
        finally:
            file_handle.close()


class RunManager:
    """Manage multiple pipeline runs"""
    
    def __init__(self, output_directory: str = "./output"):
        """
        Initialize run manager.
        
        Args:
            output_directory: Base output directory containing run subdirectories
        """
        self.output_dir = Path(output_directory)
    
    def list_runs(self) -> List[Dict[str, Any]]:
        """List all available runs with metadata"""
        runs = []
        
        for run_dir in self.output_dir.glob("run_*"):
            if run_dir.is_dir():
                # Try to read run stats
                stats_file = run_dir / "run_stats.json"
                metadata = {}
                
                if stats_file.exists():
                    try:
                        with stats_file.open('r') as f:
                            stats = json.load(f)
                            metadata = stats.get('metadata', {})
                            metadata.update(stats.get('processing_stats', {}))
                    except Exception:
                        pass
                
                runs.append({
                    'run_directory': str(run_dir),
                    'run_name': run_dir.name,
                    'created': metadata.get('timestamp', 'Unknown'),
                    'total_authors': metadata.get('total_authors', 0),
                    'total_publications': metadata.get('total_publications', 0),
                })
        
        return sorted(runs, key=lambda x: x['created'], reverse=True)
    
    def get_latest_run(self) -> Optional[DatasetQuery]:
        """Get query interface for the most recent run"""
        runs = self.list_runs()
        if runs:
            return DatasetQuery(runs[0]['run_directory'])
        return None
    
    def get_run(self, run_name: str) -> Optional[DatasetQuery]:
        """Get query interface for a specific run"""
        run_dir = self.output_dir / run_name
        if run_dir.exists():
            return DatasetQuery(str(run_dir))
        return None


# Convenience functions
def query_latest_run(output_dir: str = "./output") -> Optional[DatasetQuery]:
    """Quick access to the latest run"""
    manager = RunManager(output_dir)
    return manager.get_latest_run()


def combine_all_publications(run_directory: str, output_file: str = "all_publications.jsonl"):
    """Combine all publication chunks into a single file"""
    query = DatasetQuery(run_directory)
    
    output_path = Path(output_file)
    with output_path.open('w', encoding='utf-8') as f:
        count = 0
        for pub in query.query_publications():
            if JSON_LIB == "orjson":
                line = orjson.dumps(pub).decode('utf-8')
            else:
                line = json.dumps(pub, ensure_ascii=False)
            f.write(line + '\n')
            count += 1
    
    print(f"Combined {count} publications into {output_path}")


if __name__ == "__main__":
    # Example usage
    import argparse
    
    parser = argparse.ArgumentParser(description="Query chunked dataset")
    parser.add_argument("--run-dir", type=str, help="Run directory to query")
    parser.add_argument("--list-runs", action="store_true", help="List available runs")
    parser.add_argument("--stats", action="store_true", help="Show dataset statistics")
    parser.add_argument("--author", type=str, help="Search for author by name")
    parser.add_argument("--year", type=int, help="Find publications by year")
    parser.add_argument("--combine", action="store_true", help="Combine all chunks into single file")
    
    args = parser.parse_args()
    
    if args.list_runs:
        manager = RunManager()
        runs = manager.list_runs()
        print(f"Found {len(runs)} runs:")
        for run in runs:
            print(f"  {run['run_name']}: {run['total_authors']} authors, {run['total_publications']} publications")
    
    elif args.run_dir:
        query = DatasetQuery(args.run_dir)
        
        if args.stats:
            stats = query.get_statistics()
            print("Dataset Statistics:")
            for key, value in stats.items():
                if key not in ['top_authors_by_publications']:
                    print(f"  {key}: {value}")
        
        if args.author:
            print(f"Searching for author: {args.author}")
            for author in query.get_author_by_name(args.author):
                print(f"  {author['name']} - {author['works_count']} works")
        
        if args.year:
            print(f"Publications from {args.year}:")
            count = 0
            for pub in query.get_publications_by_year(args.year):
                print(f"  {pub['title'][:100]}...")
                count += 1
                if count >= 10:  # Limit output
                    break
        
        if args.combine:
            combine_all_publications(args.run_dir)
    
    else:
        print("Use --list-runs to see available runs or --run-dir to query a specific run")
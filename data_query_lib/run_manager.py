"""
Manager for multiple pipeline runs.
"""

import json
from contextlib import suppress
from operator import itemgetter
from pathlib import Path
from typing import Any, Dict, List, Optional

from dataset_query import DatasetQuery


class RunManager:
    """Manage multiple pipeline runs"""

    def __init__(self, output_directory: str = "../output"):
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
                    with suppress(Exception):
                        with stats_file.open("r") as f:
                            stats = json.load(f)
                            metadata = stats.get("metadata", {})
                            metadata.update(stats.get("processing_stats", {}))

                runs.append(
                    {
                        "run_directory": str(run_dir),
                        "run_name": run_dir.name,
                        "created": metadata.get("timestamp", "Unknown"),
                        "total_authors": metadata.get("total_authors", 0),
                        "total_publications": metadata.get("total_publications", 0),
                    }
                )

        return sorted(runs, key=itemgetter("created"), reverse=True)

    def get_latest_run(self) -> Optional[DatasetQuery]:
        """Get query interface for the most recent run"""
        runs = self.list_runs()
        if runs:
            return DatasetQuery(runs[0]["run_directory"])
        return None

    def get_run(self, run_name: str) -> Optional[DatasetQuery]:
        """Get query interface for a specific run"""
        run_dir = self.output_dir / run_name
        if run_dir.exists():
            return DatasetQuery(str(run_dir))
        return None

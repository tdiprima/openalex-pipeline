"""
High-performance configuration for 64-core GPU machines.
Optimized for large-scale Stony Brook dataset processing.
"""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class OptimizedAPIConfig:
    """OpenAlex API configuration optimized for high throughput"""

    base_url: str = "https://api.openalex.org"
    email: Optional[str] = None
    timeout: int = 60  # Increased for large requests
    max_retries: int = 5  # More retries for reliability
    requests_per_second: int = 10  # Polite pool allows higher rates


@dataclass
class OptimizedStonyBrookConfig:
    """Stony Brook specific configuration with comprehensive ROR coverage"""

    # Primary institution ROR IDs
    university_ror: str = "05qghxh33"  # Stony Brook University
    medicine_ror: str = "05wyq9e07"  # Stony Brook Medicine
    suny_ror: str = "01q1z8k08"  # State University of New York

    # Additional ROR IDs for comprehensive coverage
    hospital_ror: str = "02w0r2764"  # Stony Brook University Hospital

    # Pattern matching for text analysis
    abbreviations = [
        "SBU",
        "SUNY SB",
        "SUSB",
        "Stony Brook",
        "State University of New York at Stony Brook",
        "SUNY Stony Brook",
        "University at Stony Brook",
    ]


@dataclass
class OptimizedPDFConfig:
    """PDF processing optimized for parallel processing"""

    download_enabled: bool = False  # Disabled by default for large datasets
    download_timeout: int = 45  # Longer timeout for large files
    chunk_size: int = 16384  # Larger chunks for faster downloads
    max_concurrent_downloads: int = 8  # Parallel PDF downloads per author

    # Text processing
    ocr_enabled: bool = False
    summarization_enabled: bool = False
    summarization_model: str = "facebook/bart-large-cnn"
    max_summary_length: int = 150

    # GPU optimization
    use_gpu: bool = True if os.getenv("CUDA_VISIBLE_DEVICES") else False
    batch_size: int = 8  # For GPU text processing


@dataclass
class OptimizedPipelineConfig:
    """Pipeline configuration optimized for 64-core processing"""

    # Parallel processing
    max_workers: int = 64  # Optimized for CPU utilization
    batch_size: int = 100  # Authors per batch
    chunk_size: int = 2000  # Records per output file chunk

    # Default scale for comprehensive dataset
    default_num_authors: int = 5000  # Much larger default
    default_num_pubs: int = 50  # More publications per author

    # Performance tuning
    rate_limit_delay: float = 0.1  # Faster for polite pool
    memory_limit_mb: int = 16384  # 16GB memory limit

    # Output optimization
    output_directory: str = "./output"
    compression_enabled: bool = True
    compression_level: int = 6  # Good balance of speed/size
    save_pdfs: bool = False  # Disabled by default

    # Monitoring
    verbose: bool = True
    progress_reporting: bool = True
    performance_monitoring: bool = True


@dataclass
class OptimizedExportConfig:
    """Export configuration for large datasets"""

    format: str = "jsonl.gz"  # Compressed JSONL
    chunk_size: int = 2000  # Records per file
    include_abstracts: bool = True
    include_full_text: bool = False  # Only if PDFs enabled

    # Database export options (for future implementation)
    enable_database_export: bool = False
    database_batch_size: int = 1000


class OptimizedConfig:
    """High-performance configuration manager"""

    def __init__(self):
        self.api = OptimizedAPIConfig()
        self.stonybrook = OptimizedStonyBrookConfig()
        self.pdf = OptimizedPDFConfig()
        self.pipeline = OptimizedPipelineConfig()
        self.export = OptimizedExportConfig()

        # Load from environment
        self._load_from_env()

        # Performance warnings
        self._check_performance_settings()

    def _load_from_env(self):
        """Load configuration from environment variables"""
        # API configuration
        self.api.email = os.getenv("OPENALEX_EMAIL", self.api.email)

        # Performance tuning
        if os.getenv("MAX_WORKERS"):
            self.pipeline.max_workers = int(os.getenv("MAX_WORKERS"))
        if os.getenv("BATCH_SIZE"):
            self.pipeline.batch_size = int(os.getenv("BATCH_SIZE"))
        if os.getenv("CHUNK_SIZE"):
            self.pipeline.chunk_size = int(os.getenv("CHUNK_SIZE"))

        # PDF configuration
        self.pdf.download_enabled = (
            os.getenv("ENABLE_PDF_DOWNLOAD", "false").lower() == "true"
        )
        self.pdf.use_gpu = os.getenv("USE_GPU", str(self.pdf.use_gpu)).lower() == "true"

        # Memory limits
        if os.getenv("MEMORY_LIMIT_MB"):
            self.pipeline.memory_limit_mb = int(os.getenv("MEMORY_LIMIT_MB"))

        # Output configuration
        self.pipeline.output_directory = os.getenv(
            "OUTPUT_DIR", self.pipeline.output_directory
        )
        self.export.compression_enabled = (
            os.getenv("ENABLE_COMPRESSION", "true").lower() == "true"
        )

    def _check_performance_settings(self):
        """Check and warn about performance settings"""
        import multiprocessing as mp

        available_cores = mp.cpu_count()
        if self.pipeline.max_workers > available_cores:
            print(
                f"WARNING: max_workers ({self.pipeline.max_workers}) > available cores ({available_cores})"
            )

        if self.pdf.download_enabled and self.pipeline.default_num_authors > 1000:
            print(
                "WARNING: PDF downloads enabled for large dataset - this will take significant time and storage"
            )

        if not self.export.compression_enabled:
            print("WARNING: Compression disabled - output files will be very large")

    def get_performance_summary(self) -> str:
        """Get a summary of performance settings"""
        return f"""
Performance Configuration:
- Workers: {self.pipeline.max_workers}
- Batch size: {self.pipeline.batch_size}  
- Chunk size: {self.pipeline.chunk_size}
- Compression: {self.export.compression_enabled}
- PDF downloads: {self.pdf.download_enabled}
- GPU acceleration: {self.pdf.use_gpu}
- Memory limit: {self.pipeline.memory_limit_mb}MB
"""


# Global optimized config instance
optimized_config = OptimizedConfig()


# Convenience function to switch to optimized config
def use_optimized_config():
    """Switch the global config to optimized settings"""
    global config
    from config import config

    # Override key settings
    config.pipeline.default_num_authors = optimized_config.pipeline.default_num_authors
    config.pipeline.default_num_pubs = optimized_config.pipeline.default_num_pubs
    config.pdf.download_enabled = optimized_config.pdf.download_enabled

    print("Switched to optimized configuration")
    print(optimized_config.get_performance_summary())

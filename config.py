"""
Configuration module for the research pipeline.
Optional module for centralized configuration management.
"""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class APIConfig:
    """OpenAlex API configuration"""

    base_url: str = "https://api.openalex.org"
    email: Optional[str] = None
    timeout: int = 30
    max_retries: int = 3


@dataclass
class StonyBrookConfig:
    """Stony Brook specific configuration"""

    # Research Organization Registry (ROR) IDs
    university_ror: str = "05qghxh33"  # Stony Brook University
    medicine_ror: str = "05wyq9e07"  # Stony Brook Medicine
    suny_ror: str = "01q1z8k08"  # State University of New York

    # Pattern matching
    abbreviations = ["SBU", "SUNY SB"]


@dataclass
class PDFConfig:
    """PDF processing configuration"""

    download_enabled: bool = False  # Control PDF downloading - default disabled for large datasets
    download_timeout: int = 30
    chunk_size: int = 8192
    ocr_enabled: bool = False
    summarization_enabled: bool = False
    summarization_model: str = "facebook/bart-large-cnn"
    max_summary_length: int = 150


@dataclass
class PipelineConfig:
    """Main pipeline configuration"""

    default_num_authors: int = 3
    default_num_pubs: int = 2
    rate_limit_delay: float = 1.0  # seconds between API calls
    output_directory: str = "./output"
    save_pdfs: bool = True
    verbose: bool = True


class Config:
    """Central configuration manager"""

    def __init__(self):
        self.api = APIConfig()
        self.stonybrook = StonyBrookConfig()
        self.pdf = PDFConfig()
        self.pipeline = PipelineConfig()

        # Load from environment variables if available
        self._load_from_env()

    def _load_from_env(self):
        """Load configuration from environment variables"""
        # API configuration
        self.api.email = os.getenv("OPENALEX_EMAIL", self.api.email)

        # PDF configuration
        self.pdf.download_enabled = (
            os.getenv("ENABLE_PDF_DOWNLOAD", "true").lower() == "true"
        )
        self.pdf.ocr_enabled = os.getenv("ENABLE_OCR", "false").lower() == "true"
        self.pdf.summarization_enabled = (
            os.getenv("ENABLE_SUMMARIZATION", "false").lower() == "true"
        )

        # Pipeline configuration
        if os.getenv("NUM_AUTHORS"):
            self.pipeline.default_num_authors = int(os.getenv("NUM_AUTHORS"))
        if os.getenv("NUM_PUBS"):
            self.pipeline.default_num_pubs = int(os.getenv("NUM_PUBS"))

        self.pipeline.output_directory = os.getenv(
            "OUTPUT_DIR", self.pipeline.output_directory
        )
        self.pipeline.verbose = os.getenv("VERBOSE", "true").lower() == "true"


# Global config instance
config = Config()

"""
Data models for OpenAlex research pipeline.
"""

from dataclasses import dataclass
from typing import List, Optional


@dataclass
class Author:
    """Represents an OpenAlex author"""

    id: str
    name: str
    works_count: int
    cited_by_count: int
    affiliations: List[str]


@dataclass
class Publication:
    """Represents an OpenAlex work/publication"""

    id: str
    title: str
    doi: Optional[str]
    publication_year: int
    pdf_url: Optional[str]
    authors: List[str]
    abstract: Optional[str]

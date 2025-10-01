"""
Main pipeline module for coordinating the research pipeline workflow.
"""

import time
from typing import Optional

from api_client import OpenAlexAPI
from content_validator import ContentValidator
from pdf_processor import PDFProcessor


class ResearchPipeline:
    """Coordinates the entire research pipeline workflow"""

    def __init__(self, email: Optional[str] = None):
        """
        Initialize the pipeline.

        Args:
            email: Your email for OpenAlex polite pool
        """
        self.api = OpenAlexAPI(email=email)
        self.processor = PDFProcessor()
        self.validator = ContentValidator()

    def process_author(self, author, num_pubs: int = 2):
        """
        Process a single author's publications.

        Args:
            author: Author object to process
            num_pubs: Number of publications to process
        """
        print(f"\n{'=' * 70}")
        print(f"Processing: {author.name}")
        print(f"Works: {author.works_count} | Citations: {author.cited_by_count}")
        print(f"Affiliations: {', '.join(author.affiliations)}")
        print("=" * 70)

        # Get publications
        print("\nRetrieving publications...")
        publications = self.api.get_author_publications(author.id, max_results=num_pubs)

        if not publications:
            print("No publications found for this author.")
            return

        print(f"Found {len(publications)} publications.")

        # Process each publication
        for pub_idx, pub in enumerate(publications):
            self.process_publication(pub, pub_idx + 1, len(publications))

    def process_publication(self, pub, pub_idx: int, total_pubs: int):
        """
        Process a single publication.

        Args:
            pub: Publication object to process
            pub_idx: Current publication index
            total_pubs: Total number of publications
        """
        print(f"\n  --- Publication {pub_idx}/{total_pubs} ---")
        print(f"  Title: {pub.title}")
        print(f"  Year: {pub.publication_year}")
        print(f"  DOI: {pub.doi or 'N/A'}")

        # Check if PDF is available
        if pub.pdf_url:
            print("  PDF: Available ✓")
            print(f"  URL: {pub.pdf_url}")

            # Download and process PDF
            pdf_path = f"paper_{pub_idx}.pdf"
            if self.processor.download_pdf(pub.pdf_url, pdf_path):
                print(f"  Downloaded to {pdf_path}")

                text = self.processor.extract_text(pdf_path)
                print(f"  Extracted {len(text)} characters")

                # Check for Stony Brook content
                check = self.validator.check_stonybrook_content(text)
                print(f"  Stony Brook mentions: {check['count']}")
                if check["found"]:
                    print(f"  Context: {check['contexts'][0]}")

                # Summarize
                summary = self.processor.summarize_text(text)
                if summary:
                    print(f"  Summary: {summary}")

        else:
            print("  PDF: Not available ✗")

        # Show abstract if available
        if pub.abstract:
            abstract_preview = (
                pub.abstract[:200] + "..." if len(pub.abstract) > 200 else pub.abstract
            )
            print(f"  Abstract: {abstract_preview}")

    def run(self, num_authors: int = 3, num_pubs: int = 2):
        """
        Run the complete pipeline.

        Args:
            num_authors: Number of authors to process
            num_pubs: Number of publications per author
        """
        print("=" * 70)
        print("STONY BROOK UNIVERSITY RESEARCH PIPELINE")
        print("=" * 70)

        # Step 1: Find Stony Brook authors
        print("\n[STEP 1] Finding Stony Brook authors...")
        authors = self.api.find_stonybrook_authors(max_results=num_authors)

        if not authors:
            print("No authors found. Exiting.")
            return

        print(f"\nFound {len(authors)} authors.")

        # Step 2: Process each author
        for idx, author in enumerate(authors):
            print(f"\n[AUTHOR {idx+1}/{len(authors)}]")
            self.process_author(author, num_pubs)

            # Rate limiting
            time.sleep(1)

        print("\n" + "=" * 70)
        print("PIPELINE COMPLETE")
        print("=" * 70)

"""
Main pipeline module for coordinating the research pipeline workflow.
"""

import time
from typing import Optional

from api_client import OpenAlexAPI
from config import config
from content_validator import ContentValidator
from data_exporter_streaming import DataExporter
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
        self.exporter = DataExporter(output_dir=config.pipeline.output_directory)

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

        # Process each publication and collect results
        processing_results = []
        for pub_idx, pub in enumerate(publications):
            result = self.process_publication(pub, pub_idx + 1, len(publications))
            processing_results.append(result)

        # Add to exporter
        self.exporter.add_author(author, publications, processing_results)

    def process_publication(self, pub, pub_idx: int, total_pubs: int):
        """
        Process a single publication.

        Args:
            pub: Publication object to process
            pub_idx: Current publication index
            total_pubs: Total number of publications

        Returns:
            Dict with processing results
        """
        from datetime import datetime

        # Initialize result dictionary
        result = {
            "timestamp": datetime.now().isoformat(),
            "pdf_downloaded": False,
            "pdf_path": None,
            "text_extracted": False,
            "text_length": 0,
            "stonybrook_validation": {},
            "summary": None,
        }

        print(f"\n  --- Publication {pub_idx}/{total_pubs} ---")
        print(f"  Title: {pub.title}")
        print(f"  Year: {pub.publication_year}")
        print(f"  DOI: {pub.doi or 'N/A'}")

        # Check if PDF is available and downloads are enabled
        if pub.pdf_url:
            print("  PDF: Available ✓")
            print(f"  URL: {pub.pdf_url}")

            if config.pdf.download_enabled:
                # Download and process PDF
                # Create organized PDF path in output directory
                run_dir = self.exporter.get_run_directory()
                pdf_filename = f"paper_{pub_idx}_{pub.title[:50].replace('/', '_').replace(':', '_')}.pdf"
                pdf_path = f"{run_dir}/{pdf_filename}"

                if self.processor.download_pdf(pub.pdf_url, pdf_path):
                    print(f"  Downloaded to {pdf_path}")
                    result["pdf_downloaded"] = True
                    result["pdf_path"] = pdf_path

                    text = self.processor.extract_text(pdf_path)
                    print(f"  Extracted {len(text)} characters")
                    result["text_extracted"] = len(text) > 0
                    result["text_length"] = len(text)

                    # Check for Stony Brook content
                    check = self.validator.check_stonybrook_content(text)
                    print(f"  Stony Brook mentions: {check['count']}")
                    if check["found"]:
                        print(f"  Context: {check['contexts'][0]}")
                    result["stonybrook_validation"] = check

                    # Summarize (if enabled)
                    if config.pdf.summarization_enabled:
                        summary = self.processor.summarize_text(text)
                        if summary:
                            print(f"  Summary: {summary}")
                            result["summary"] = summary
            else:
                print("  PDF downloads disabled - collecting metadata only")

        elif pub.pdf_url is None:
            print("  PDF: Not available ✗")
        else:
            print("  PDF: Not available ✗")

        # Show abstract if available
        if pub.abstract:
            abstract_preview = (
                pub.abstract[:200] + "..." if len(pub.abstract) > 200 else pub.abstract
            )
            print(f"  Abstract: {abstract_preview}")

        return result

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

        # Finalize and save export statistics
        print("\n[FINALIZING EXPORT]")
        stats_path = self.exporter.save_stats()
        stats = self.exporter.get_stats()
        self.exporter.close()

        print("\nResults exported to:")
        print(f"  Authors: {self.exporter.authors_file_path}")
        print(f"  Publications: {self.exporter.publications_file_path}")
        print(f"  Statistics: {stats_path}")
        print(f"  Run directory: {self.exporter.get_run_directory()}")

        print("\nProcessing Statistics:")
        print(f"  Authors processed: {stats['total_authors']}")
        print(f"  Publications found: {stats['total_publications']}")
        print(f"  PDFs downloaded: {stats['pdfs_downloaded']}")
        print(f"  Stony Brook mentions: {stats['stonybrook_mentions_found']}")
        print(f"  Summaries generated: {stats['summaries_generated']}")

        print("\n" + "=" * 70)
        print("PIPELINE COMPLETE")
        print("=" * 70)

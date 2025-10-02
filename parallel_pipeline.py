"""
Parallel-optimized pipeline for high-performance author and publication processing.
Designed for 32-core machines with GPU acceleration.
"""

import asyncio
import concurrent.futures
import time
from typing import List, Optional, Tuple
import multiprocessing as mp

from api_client import OpenAlexAPI
from config import config
from content_validator import ContentValidator
from data_exporter_streaming import StreamingDataExporter
from pdf_processor import PDFProcessor
from models import Author, Publication


class ParallelResearchPipeline:
    """High-performance parallel pipeline for large-scale processing"""

    def __init__(self, email: Optional[str] = None, max_workers: Optional[int] = None):
        """
        Initialize parallel pipeline.

        Args:
            email: Your email for OpenAlex polite pool
            max_workers: Number of worker processes (default: CPU count)
        """
        self.email = email
        
        # Optimize for 32-core machine
        if max_workers is None:
            max_workers = min(32, mp.cpu_count())
        self.max_workers = max_workers
        
        # Initialize components (one per process will be created)
        self.api = OpenAlexAPI(email=email)
        
        # Use compressed chunked exporter for large datasets
        self.exporter = StreamingDataExporter(
            output_dir=config.pipeline.output_directory,
            compress=True,
            chunk_size=1000
        )
        
        print(f"Initialized parallel pipeline with {self.max_workers} workers")

    def _process_author_batch(self, author_batch: List[Author], num_pubs: int) -> List[Tuple[Author, List[Publication], List]]:
        """
        Process a batch of authors in parallel.
        
        Args:
            author_batch: List of authors to process
            num_pubs: Number of publications per author
            
        Returns:
            List of (author, publications, processing_results) tuples
        """
        results = []
        
        # Process authors concurrently using ThreadPoolExecutor for I/O bound tasks
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all author processing tasks
            future_to_author = {
                executor.submit(self._process_single_author, author, num_pubs): author
                for author in author_batch
            }
            
            # Collect results as they complete
            for future in concurrent.futures.as_completed(future_to_author):
                author = future_to_author[future]
                try:
                    result = future.result()
                    if result:
                        results.append(result)
                        print(f"✓ Completed: {author.name} ({len(result[1])} publications)")
                except Exception as e:
                    print(f"✗ Error processing {author.name}: {e}")
        
        return results

    def _process_single_author(self, author: Author, num_pubs: int) -> Optional[Tuple[Author, List[Publication], List]]:
        """
        Process a single author (designed to run in separate thread).
        
        Args:
            author: Author to process
            num_pubs: Number of publications to process
            
        Returns:
            Tuple of (author, publications, processing_results) or None if failed
        """
        try:
            # Create fresh API client for this thread
            thread_api = OpenAlexAPI(email=self.email)
            thread_processor = PDFProcessor()
            thread_validator = ContentValidator()
            
            # Get publications
            publications = thread_api.get_author_publications(author.id, max_results=num_pubs)
            
            if not publications:
                return None
            
            # Process publications concurrently within this author
            processing_results = []
            
            # For PDF-enabled processing, use ThreadPoolExecutor for concurrent downloads
            if config.pdf.download_enabled:
                with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pdf_executor:
                    future_to_pub = {
                        pdf_executor.submit(self._process_single_publication, pub, idx + 1, len(publications), thread_processor, thread_validator): pub
                        for idx, pub in enumerate(publications)
                    }
                    
                    for future in concurrent.futures.as_completed(future_to_pub):
                        try:
                            result = future.result()
                            processing_results.append(result)
                        except Exception as e:
                            print(f"Error processing publication: {e}")
                            processing_results.append(self._create_empty_result())
            else:
                # Metadata-only processing (much faster)
                for idx, pub in enumerate(publications):
                    result = self._process_single_publication(pub, idx + 1, len(publications), thread_processor, thread_validator)
                    processing_results.append(result)
            
            return (author, publications, processing_results)
            
        except Exception as e:
            print(f"Error in _process_single_author for {author.name}: {e}")
            return None

    def _process_single_publication(self, pub: Publication, pub_idx: int, total_pubs: int, processor: PDFProcessor, validator: ContentValidator) -> dict:
        """
        Process a single publication.
        
        Args:
            pub: Publication to process
            pub_idx: Publication index
            total_pubs: Total publications for this author
            processor: PDF processor instance
            validator: Content validator instance
            
        Returns:
            Processing result dictionary
        """
        from datetime import datetime
        
        result = {
            "timestamp": datetime.now().isoformat(),
            "pdf_downloaded": False,
            "pdf_path": None,
            "text_extracted": False,
            "text_length": 0,
            "stonybrook_validation": {},
            "summary": None,
        }
        
        # Only process PDFs if enabled and available
        if pub.pdf_url and config.pdf.download_enabled:
            try:
                # Create organized PDF path
                run_dir = self.exporter.get_run_directory()
                pdf_filename = f"paper_{pub_idx}_{pub.title[:50].replace('/', '_').replace(':', '_')}.pdf"
                pdf_path = f"{run_dir}/{pdf_filename}"
                
                if processor.download_pdf(pub.pdf_url, pdf_path):
                    result["pdf_downloaded"] = True
                    result["pdf_path"] = pdf_path
                    
                    text = processor.extract_text(pdf_path)
                    result["text_extracted"] = len(text) > 0
                    result["text_length"] = len(text)
                    
                    # Validate Stony Brook content
                    check = validator.check_stonybrook_content(text)
                    result["stonybrook_validation"] = check
                    
                    # Summarize if enabled
                    if config.pdf.summarization_enabled:
                        summary = processor.summarize_text(text)
                        if summary:
                            result["summary"] = summary
                            
            except Exception as e:
                print(f"Error processing PDF for {pub.title}: {e}")
        
        return result

    def _create_empty_result(self) -> dict:
        """Create empty processing result for failed publications"""
        from datetime import datetime
        return {
            "timestamp": datetime.now().isoformat(),
            "pdf_downloaded": False,
            "pdf_path": None,
            "text_extracted": False,
            "text_length": 0,
            "stonybrook_validation": {},
            "summary": None,
        }

    async def run_async(self, num_authors: int = 100, num_pubs: int = 10, batch_size: int = 50):
        """
        Run the pipeline asynchronously with batched processing.
        
        Args:
            num_authors: Total number of authors to process
            num_pubs: Number of publications per author
            batch_size: Number of authors to process in each batch
        """
        print("=" * 70)
        print("STONY BROOK UNIVERSITY PARALLEL RESEARCH PIPELINE")
        print(f"Workers: {self.max_workers} | Batch size: {batch_size}")
        print("=" * 70)
        
        start_time = time.time()
        
        # Step 1: Find authors
        print(f"\n[STEP 1] Finding {num_authors} Stony Brook authors...")
        authors = self.api.find_stonybrook_authors(max_results=num_authors)
        
        if not authors:
            print("No authors found. Exiting.")
            return
        
        print(f"Found {len(authors)} authors. Processing in batches of {batch_size}...")
        
        # Step 2: Process authors in batches
        total_processed = 0
        for batch_start in range(0, len(authors), batch_size):
            batch_end = min(batch_start + batch_size, len(authors))
            batch = authors[batch_start:batch_end]
            batch_num = (batch_start // batch_size) + 1
            total_batches = (len(authors) + batch_size - 1) // batch_size
            
            print(f"\n[BATCH {batch_num}/{total_batches}] Processing authors {batch_start+1}-{batch_end}")
            
            # Process batch in parallel
            batch_results = self._process_author_batch(batch, num_pubs)
            
            # Stream results to exporter immediately
            for author, publications, processing_results in batch_results:
                self.exporter.add_author(author, publications, processing_results)
                total_processed += 1
            
            print(f"Batch {batch_num} complete. Total processed: {total_processed}/{len(authors)}")
            
            # Brief pause between batches to avoid overwhelming APIs
            await asyncio.sleep(0.5)
        
        # Finalize
        print(f"\n[FINALIZING] Processed {total_processed} authors")
        stats_path = self.exporter.save_stats()
        stats = self.exporter.get_stats()
        self.exporter.close()
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        # Results summary
        print(f"\nResults exported to: {self.exporter.get_run_directory()}")
        print(f"Statistics saved to: {stats_path}")
        
        print(f"\nPerformance Statistics:")
        print(f"  Total processing time: {processing_time:.1f} seconds")
        print(f"  Authors per second: {total_processed / processing_time:.2f}")
        print(f"  Publications found: {stats['total_publications']}")
        print(f"  PDFs downloaded: {stats['pdfs_downloaded']}")
        print(f"  Stony Brook mentions: {stats['stonybrook_mentions_found']}")
        
        print("\n" + "=" * 70)
        print("PARALLEL PIPELINE COMPLETE")
        print("=" * 70)

    def run(self, num_authors: int = 100, num_pubs: int = 10, batch_size: int = 50):
        """
        Synchronous wrapper for async pipeline.
        
        Args:
            num_authors: Total number of authors to process
            num_pubs: Number of publications per author  
            batch_size: Number of authors to process in each batch
        """
        asyncio.run(self.run_async(num_authors, num_pubs, batch_size))


if __name__ == "__main__":
    # Example usage for high-performance processing
    import argparse
    
    parser = argparse.ArgumentParser(description="Parallel OpenAlex Pipeline")
    parser.add_argument("--email", type=str, required=True, help="Your email for OpenAlex")
    parser.add_argument("--authors", type=int, default=100, help="Number of authors")
    parser.add_argument("--pubs", type=int, default=10, help="Publications per author")
    parser.add_argument("--batch-size", type=int, default=50, help="Batch size for parallel processing")
    parser.add_argument("--workers", type=int, help="Number of worker processes")
    
    args = parser.parse_args()
    
    pipeline = ParallelResearchPipeline(
        email=args.email,
        max_workers=args.workers
    )
    
    pipeline.run(
        num_authors=args.authors,
        num_pubs=args.pubs,
        batch_size=args.batch_size
    )
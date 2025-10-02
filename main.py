"""
Main entry point for the OpenAlex research pipeline.

To get truly "all" authors:

# Very large dataset (be careful!)
python main.py --email your@email.com --authors 50000 --pubs 500 --optimized --parallel

Safe recommendations:

# Test run
python main.py --email your@email.com --authors 100 --pubs 10 --optimized --parallel

# Medium scale  
python main.py --email your@email.com --authors 1000 --pubs 25 --optimized --parallel

# Large scale (might take hours)
python main.py --email your@email.com --authors 10000 --pubs 50 --optimized --parallel

The OpenAlex API doesn't have a true "get all authors" option - you specify a max number. For Stony
Brook, there are likely 5,000-15,000 total authors in their database.
"""

import argparse
import os

from pipeline import ResearchPipeline


def main():
    """Main function to run the pipeline with command-line arguments"""

    parser = argparse.ArgumentParser(
        description="OpenAlex API Pipeline for Stony Brook University Research"
    )
    parser.add_argument(
        "--email",
        type=str,
        default=os.getenv("EMAIL", "your.email@example.com"),
        help="Your email for OpenAlex polite pool (faster access)",
    )
    parser.add_argument(
        "--authors",
        type=int,
        default=3,
        help="Number of authors to process (default: 3)",
    )
    parser.add_argument(
        "--pubs",
        type=int,
        default=2,
        help="Number of publications per author (default: 2)",
    )
    parser.add_argument(
        "--no-pdf",
        action="store_true",
        help="Skip PDF downloads (metadata only)",
    )
    parser.add_argument(
        "--optimized",
        action="store_true",
        help="Use optimized configuration for large datasets",
    )
    parser.add_argument(
        "--parallel",
        action="store_true",
        help="Use parallel pipeline for high-performance processing",
    )

    args = parser.parse_args()

    # Load configuration (optimized if requested)
    if args.optimized:
        from config_optimized import use_optimized_config
        use_optimized_config()
        print("Using optimized configuration for large datasets")
    
    from config import config

    if args.no_pdf:
        config.pdf.download_enabled = False

    print("Starting pipeline...")
    print(f"Configuration: {args.authors} authors, {args.pubs} publications each")
    print(f"Email: {args.email}")
    print(f"PDF downloads: {'Disabled' if args.no_pdf else 'Enabled'}")

    # Create and run pipeline (parallel if requested)
    if args.parallel:
        from parallel_pipeline import ParallelResearchPipeline
        pipeline = ParallelResearchPipeline(email=args.email)
        # Use larger defaults for parallel processing
        batch_size = max(50, args.authors // 10)
        pipeline.run(num_authors=args.authors, num_pubs=args.pubs, batch_size=batch_size)
    else:
        pipeline = ResearchPipeline(email=args.email)
        pipeline.run(num_authors=args.authors, num_pubs=args.pubs)


if __name__ == "__main__":
    main()

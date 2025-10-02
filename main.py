"""
Main entry point for the OpenAlex research pipeline.
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
        pipeline.run(
            num_authors=args.authors, num_pubs=args.pubs, batch_size=batch_size
        )
    else:
        pipeline = ResearchPipeline(email=args.email)
        pipeline.run(num_authors=args.authors, num_pubs=args.pubs)


if __name__ == "__main__":
    main()

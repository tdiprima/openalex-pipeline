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

    args = parser.parse_args()

    print("Starting pipeline...")
    print(f"Configuration: {args.authors} authors, {args.pubs} publications each")
    print(f"Email: {args.email}")

    # Create and run pipeline
    pipeline = ResearchPipeline(email=args.email)
    pipeline.run(num_authors=args.authors, num_pubs=args.pubs)


if __name__ == "__main__":
    main()

"""
Check database for authors from CSV and export their publications.
Outputs: lastname, firstname, department, and all publication details.
"""

import asyncio
import csv
import json
import os
from urllib.parse import quote

import asyncpg
from dotenv import load_dotenv

load_dotenv()


def reconstruct_abstract(abstract_str):
    """
    Convert OpenAlex inverted index format to readable text.
    The abstract is stored as a JSON-serialized dict like:
    {"word1": [0], "word2": [1], ...}
    (older rows may use the Python repr form, which is also accepted).
    """
    if not abstract_str:
        return ""

    try:
        try:
            inverted_index = json.loads(abstract_str)
        except ValueError:
            import ast

            inverted_index = ast.literal_eval(abstract_str)

        # Find the maximum position to know how long the text is
        max_pos = 0
        for positions in inverted_index.values():
            if positions:
                max_pos = max(max_pos, max(positions))

        # Create an array to hold words at each position
        words = [""] * (max_pos + 1)

        # Place each word at its positions
        for word, positions in inverted_index.items():
            for pos in positions:
                words[pos] = word

        # Join the words with spaces
        return " ".join(words)
    except (ValueError, SyntaxError, KeyError, TypeError, AttributeError, IndexError):
        # If parsing fails, return empty string
        return ""


async def check_profiles():
    """Check if CSV profiles have publications in the database and export to CSV"""

    # Connect to database
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST", "localhost")
    db_name = os.getenv("DB_NAME")

    db_url = f"postgresql://{db_user}:{quote(db_password, safe="")}@{db_host}/{db_name}"
    conn = await asyncpg.connect(db_url)

    try:
        # Read CSV and build search patterns
        print("📄 Reading profiles from CSV...")
        profiles = []

        with open("authors_with_pubs_found.csv", "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                lastname = row["Lastname"].strip()
                firstname = row["Firstname"].strip()
                department = row["departments"].strip()
                profiles.append((lastname, firstname, department))

        print(f"Found {len(profiles)} profiles to check\n")

        # Prepare output CSV
        output_file = "authors_publications_export.csv"
        output_rows = []

        # Query database efficiently
        print("🔍 Searching database for matching authors and their publications...\n")

        total_pubs_found = 0

        for i, (lastname, firstname, department) in enumerate(profiles, 1):
            # Case-insensitive search for name variations
            # OpenAlex names are in format "Firstname Lastname"
            name_pattern = f"%{firstname}%{lastname}%"

            # Find matching authors
            authors = await conn.fetch(
                """
                SELECT id
                FROM authors
                WHERE LOWER(name) LIKE LOWER($1)
                """,
                name_pattern,
            )

            if authors:
                # Get publications for these authors, joined by OpenAlex author ID
                author_ids = [a["id"] for a in authors]

                publications = await conn.fetch(
                    """
                    SELECT title, doi, publication_year, pdf_url, authors, abstract
                    FROM publications
                    WHERE author_ids && $1
                    ORDER BY publication_year DESC
                    """,
                    author_ids,
                )

                if publications:
                    for pub in publications:
                        # Reconstruct abstract from inverted index format
                        abstract = reconstruct_abstract(pub["abstract"])

                        output_rows.append(
                            {
                                "lastname": lastname,
                                "firstname": firstname,
                                "department": department,
                                "title": pub["title"],
                                "doi": pub["doi"] or "",
                                "publication_year": pub["publication_year"],
                                "pdf_url": pub["pdf_url"] or "",
                                "authors": (
                                    "; ".join(pub["authors"]) if pub["authors"] else ""
                                ),
                                "abstract": abstract,
                            }
                        )

                    total_pubs_found += len(publications)
                    print(
                        f"[{i:3d}/{len(profiles)}] {firstname} {lastname:20s} | ✅ Found {len(publications)} publications"
                    )
                else:
                    print(
                        f"[{i:3d}/{len(profiles)}] {firstname} {lastname:20s} | ⚠️  Matched author but NO publications"
                    )
            else:
                print(
                    f"[{i:3d}/{len(profiles)}] {firstname} {lastname:20s} | ❌ NOT FOUND"
                )

        # Write to CSV
        if output_rows:
            print(
                f"\n📝 Writing {len(output_rows)} publication records to {output_file}..."
            )
            with open(output_file, "w", encoding="utf-8", newline="") as f:
                fieldnames = [
                    "lastname",
                    "firstname",
                    "department",
                    "title",
                    "doi",
                    "publication_year",
                    "pdf_url",
                    "authors",
                    "abstract",
                ]
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(output_rows)
            print("✅ Export complete!")
        else:
            print("\n⚠️  No publications found to export.")

        # Summary Report
        print("\n" + "=" * 70)
        print("📊 SUMMARY REPORT")
        print("=" * 70)
        print(f"Total profiles checked: {len(profiles)}")
        print(f"Total publications exported: {total_pubs_found}")
        print(f"Output file: {output_file}")
        print("=" * 70)

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(check_profiles())

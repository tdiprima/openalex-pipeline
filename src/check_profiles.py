"""
Check if profiles without publications actually have publications in our database.
Memory-efficient: uses streaming CSV reading and batched DB queries.
"""

import asyncio
import csv
import os
from urllib.parse import quote_plus

import asyncpg
from dotenv import load_dotenv

load_dotenv()


async def check_profiles():
    """Check if CSV profiles have publications in the database"""

    # Connect to database
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST", "localhost")
    db_name = os.getenv("DB_NAME")

    db_url = f"postgresql://{db_user}:{quote_plus(db_password)}@{db_host}/{db_name}"
    conn = await asyncpg.connect(db_url)

    try:
        # Read CSV and build search patterns
        print("📄 Reading profiles from CSV...")
        profiles = []

        with open("profiles_without_publications.csv", "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                lastname = row["Lastname"].strip()
                firstname = row["Firstname"].strip()
                department = row["Departments"].strip()
                profiles.append((lastname, firstname, department))

        print(f"Found {len(profiles)} profiles to check\n")

        # Query database efficiently
        # Strategy: Search authors by name patterns, then check if they have publications
        print("🔍 Searching database for matching authors with publications...\n")

        found_with_pubs = []
        not_found = []
        found_no_pubs = []

        # Use a prepared statement for efficiency
        # Check both: 1) author exists in authors table, 2) author has publications
        for i, (lastname, firstname, department) in enumerate(profiles, 1):
            # Case-insensitive search for name variations
            # OpenAlex names are in format "Firstname Lastname"
            name_pattern = f"%{firstname}%{lastname}%"

            # Find matching authors
            authors = await conn.fetch(
                """
                SELECT id, name, works_count
                FROM authors
                WHERE LOWER(name) LIKE LOWER($1)
                """,
                name_pattern,
            )

            if authors:
                # Check if ANY of these authors have publications in our publications table
                author_names = [a["name"] for a in authors]

                pub_count = await conn.fetchval(
                    """
                    SELECT COUNT(DISTINCT p.id)
                    FROM publications p
                    WHERE authors && $1
                    """,
                    author_names,  # Check if any matched author names appear in publications
                )

                # Also check OpenAlex works_count
                max_works = max(a["works_count"] for a in authors)

                if pub_count > 0 or max_works > 0:
                    found_with_pubs.append(
                        {
                            "profile": f"{firstname} {lastname}",
                            "department": department,
                            "matched_authors": [
                                {"name": a["name"], "works": a["works_count"]}
                                for a in authors
                            ],
                            "pubs_in_db": pub_count,
                        }
                    )
                    status = "✅ HAS PUBS"
                else:
                    found_no_pubs.append(f"{firstname} {lastname} ({department})")
                    status = "⚠️  NO PUBS"

                print(
                    f"[{i:3d}/{len(profiles)}] {firstname} {lastname:20s} | {status} | Matched: {len(authors)} author(s)"
                )
            else:
                not_found.append(f"{firstname} {lastname} ({department})")
                print(
                    f"[{i:3d}/{len(profiles)}] {firstname} {lastname:20s} | ❌ NOT FOUND"
                )

        # Summary Report
        print("\n" + "=" * 70)
        print("📊 SUMMARY REPORT")
        print("=" * 70)
        print(f"Total profiles checked: {len(profiles)}")
        print(f"✅ Found WITH publications: {len(found_with_pubs)}")
        print(f"⚠️  Found but NO publications: {len(found_no_pubs)}")
        print(f"❌ Not found in database: {len(not_found)}")
        print("=" * 70)

        # Detailed results for those WITH publications
        if found_with_pubs:
            print(
                f"\n🎉 PROFILES THAT ACTUALLY HAVE PUBLICATIONS ({len(found_with_pubs)}):"
            )
            print("-" * 70)
            for item in found_with_pubs:
                print(f"\n{item['profile']} - {item['department']}")
                print(f"  Publications in our DB: {item['pubs_in_db']}")
                print("  Matched authors:")
                for author in item["matched_authors"]:
                    print(f"    - {author['name']} (OpenAlex works: {author['works']})")

        # Sample of not found (if too many, show first 20)
        if not_found:
            print(f"\n❌ NOT FOUND IN DATABASE (showing first 20 of {len(not_found)}):")
            print("-" * 70)
            for profile in not_found[:20]:
                print(f"  {profile}")
            if len(not_found) > 20:
                print(f"  ... and {len(not_found) - 20} more")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(check_profiles())

"""
Check database for authors from CSV and export their publications.
Outputs: lastname, firstname, department, and all publication details.

Identity resolution is separated from export:
  * each CSV profile is resolved to explicit OpenAlex author IDs, persisted in
    the profile_author_map table (status 'resolved' or 'ambiguous');
  * profiles with a single candidate are auto-resolved, profiles with several
    are flagged and written to ambiguous_profiles.csv for manual resolution
    (set status='resolved' on the correct row(s) in profile_author_map);
  * only resolved mappings are exported.
"""

import asyncio
import csv
import json
import os
import sys
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


PROFILES_CSV = "authors_with_pubs_found.csv"
OUTPUT_CSV = "authors_publications_export.csv"
AMBIGUOUS_CSV = "ambiguous_profiles.csv"

EXPORT_FIELDS = [
    "lastname",
    "firstname",
    "department",
    "author_id",
    "title",
    "doi",
    "publication_year",
    "pdf_url",
    "authors",
    "abstract",
]


class ExportNotReady(RuntimeError):
    pass


async def ensure_mapping_table(conn):
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS profile_author_map (
            lastname TEXT NOT NULL,
            firstname TEXT NOT NULL,
            author_id TEXT NOT NULL REFERENCES authors(id),
            author_name TEXT,
            status TEXT NOT NULL,          -- 'resolved' | 'ambiguous' | 'rejected'
            resolved_by TEXT,              -- 'auto' | 'manual'
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (lastname, firstname, author_id)
        )
        """
    )


async def check_export_readiness(conn, force: bool = False):
    """Refuse to export from a database whose ingestion is incomplete or
    whose rows predate the author_ids column, unless forced."""
    problems = []

    has_col = await conn.fetchval(
        """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'publications' AND column_name = 'author_ids'
        )
        """
    )
    if not has_col:
        problems.append(
            "publications.author_ids column missing: run openalex_pipeline.py first"
        )
    else:
        null_count = await conn.fetchval(
            "SELECT COUNT(*) FROM publications WHERE author_ids IS NULL"
        )
        if null_count:
            problems.append(
                f"{null_count} publications have no author_ids "
                "(ingested before schema v2): run `openalex_pipeline.py --backfill`"
            )

    has_runs = await conn.fetchval(
        "SELECT to_regclass('ingestion_runs') IS NOT NULL"
    )
    if has_runs:
        last = await conn.fetchrow(
            "SELECT id, status, started_at FROM ingestion_runs ORDER BY id DESC LIMIT 1"
        )
        if last is None:
            problems.append("no ingestion run recorded")
        elif last["status"] != "complete":
            problems.append(
                f"last ingestion run #{last['id']} ({last['started_at']:%Y-%m-%d}) "
                f"is '{last['status']}'"
            )
        incomplete = await conn.fetchval(
            "SELECT COUNT(*) FROM author_ingestion_status WHERE status <> 'complete'"
        )
        if incomplete:
            problems.append(f"{incomplete} authors are not marked complete")
    else:
        problems.append("no ingestion_runs table: ingestion completion is unknown")

    if problems:
        msg = "Export readiness check failed:\n  - " + "\n  - ".join(problems)
        if force:
            print(f"⚠️  {msg}\n   Continuing because --force was given.\n")
        else:
            raise ExportNotReady(msg + "\nRe-run with --force to export anyway.")


async def resolve_profile(conn, lastname: str, firstname: str):
    """Return (resolved_author_ids, ambiguous_candidates) for a profile.

    Existing 'resolved' mappings win. Otherwise a single name match is
    auto-resolved; multiple matches are recorded as 'ambiguous'.
    """
    existing = await conn.fetch(
        """
        SELECT author_id, status FROM profile_author_map
        WHERE lastname = $1 AND firstname = $2
        """,
        lastname,
        firstname,
    )
    resolved = [r["author_id"] for r in existing if r["status"] == "resolved"]
    if resolved:
        return resolved, []
    if existing:
        # Previously flagged ambiguous and still unresolved
        candidates = await conn.fetch(
            """
            SELECT a.id, a.name, a.works_count, a.affiliations
            FROM authors a
            JOIN profile_author_map m ON m.author_id = a.id
            WHERE m.lastname = $1 AND m.firstname = $2 AND m.status = 'ambiguous'
            """,
            lastname,
            firstname,
        )
        return [], candidates

    candidates = await conn.fetch(
        """
        SELECT id, name, works_count, affiliations
        FROM authors
        WHERE LOWER(name) LIKE LOWER($1)
        """,
        f"%{firstname}%{lastname}%",
    )
    if not candidates:
        return [], []

    status = "resolved" if len(candidates) == 1 else "ambiguous"
    await conn.executemany(
        """
        INSERT INTO profile_author_map
            (lastname, firstname, author_id, author_name, status, resolved_by)
        VALUES ($1, $2, $3, $4, $5, 'auto')
        ON CONFLICT (lastname, firstname, author_id) DO NOTHING
        """,
        [(lastname, firstname, c["id"], c["name"], status) for c in candidates],
    )
    if status == "resolved":
        return [candidates[0]["id"]], []
    return [], candidates


async def check_profiles(force: bool = False):
    """Resolve CSV profiles to author IDs, then stream their publications to CSV."""

    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST", "localhost")
    db_name = os.getenv("DB_NAME")

    db_url = f"postgresql://{db_user}:{quote(db_password, safe='')}@{db_host}/{db_name}"
    conn = await asyncpg.connect(db_url)

    try:
        await ensure_mapping_table(conn)
        await check_export_readiness(conn, force=force)

        print("📄 Reading profiles from CSV...")
        profiles = []
        with open(PROFILES_CSV, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                profiles.append(
                    (
                        row["Lastname"].strip(),
                        row["Firstname"].strip(),
                        row["departments"].strip(),
                    )
                )
        print(f"Found {len(profiles)} profiles to check\n")

        print("🔍 Resolving profiles and streaming publications...\n")

        total_pubs_found = 0
        ambiguous_rows = []
        counts = {"exported": 0, "no_pubs": 0, "ambiguous": 0, "not_found": 0}

        with open(OUTPUT_CSV, "w", encoding="utf-8", newline="") as out:
            writer = csv.DictWriter(out, fieldnames=EXPORT_FIELDS)
            writer.writeheader()

            for i, (lastname, firstname, department) in enumerate(profiles, 1):
                label = f"[{i:3d}/{len(profiles)}] {firstname} {lastname:20s} |"
                author_ids, candidates = await resolve_profile(conn, lastname, firstname)

                if candidates:
                    counts["ambiguous"] += 1
                    for c in candidates:
                        ambiguous_rows.append(
                            {
                                "lastname": lastname,
                                "firstname": firstname,
                                "department": department,
                                "author_id": c["id"],
                                "author_name": c["name"],
                                "works_count": c["works_count"],
                                "affiliations": "; ".join(c["affiliations"] or []),
                            }
                        )
                    print(f"{label} ❓ AMBIGUOUS ({len(candidates)} candidates)")
                    continue

                if not author_ids:
                    counts["not_found"] += 1
                    print(f"{label} ❌ NOT FOUND")
                    continue

                # Stream rows via a server-side cursor; nothing accumulates in memory.
                n = 0
                async with conn.transaction():
                    async for pub in conn.cursor(
                        """
                        SELECT id, title, doi, publication_year, pdf_url, authors,
                               author_ids, abstract
                        FROM publications
                        WHERE author_ids && $1
                        ORDER BY publication_year DESC
                        """,
                        author_ids,
                    ):
                        matched = next(
                            (a for a in pub["author_ids"] if a in author_ids), ""
                        )
                        writer.writerow(
                            {
                                "lastname": lastname,
                                "firstname": firstname,
                                "department": department,
                                "author_id": matched,
                                "title": pub["title"],
                                "doi": pub["doi"] or "",
                                "publication_year": pub["publication_year"],
                                "pdf_url": pub["pdf_url"] or "",
                                "authors": "; ".join(pub["authors"] or []),
                                "abstract": reconstruct_abstract(pub["abstract"]),
                            }
                        )
                        n += 1

                total_pubs_found += n
                if n:
                    counts["exported"] += 1
                    print(f"{label} ✅ Found {n} publications")
                else:
                    counts["no_pubs"] += 1
                    print(f"{label} ⚠️  Resolved author but NO publications")

        if ambiguous_rows:
            with open(AMBIGUOUS_CSV, "w", encoding="utf-8", newline="") as f:
                w = csv.DictWriter(f, fieldnames=list(ambiguous_rows[0].keys()))
                w.writeheader()
                w.writerows(ambiguous_rows)

        print("\n" + "=" * 70)
        print("📊 SUMMARY REPORT")
        print("=" * 70)
        print(f"Total profiles checked:      {len(profiles)}")
        print(f"Profiles exported:           {counts['exported']}")
        print(f"Resolved, no publications:   {counts['no_pubs']}")
        print(f"Ambiguous (not exported):    {counts['ambiguous']}")
        print(f"Not found:                   {counts['not_found']}")
        print(f"Total publications exported: {total_pubs_found}")
        print(f"Output file: {OUTPUT_CSV}")
        if ambiguous_rows:
            print(
                f"Ambiguous candidates: {AMBIGUOUS_CSV} — resolve by setting "
                "status='resolved' in profile_author_map, then re-run"
            )
        print("=" * 70)

    finally:
        await conn.close()


if __name__ == "__main__":
    try:
        asyncio.run(check_profiles(force="--force" in sys.argv))
    except ExportNotReady as e:
        print(f"❌ {e}")
        sys.exit(1)

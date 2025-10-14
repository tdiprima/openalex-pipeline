import asyncio
import csv
import json
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from itertools import starmap
from typing import Dict, List, Optional

import aiohttp


class PubMedAuthorSearch:
    def __init__(self, email: str, api_key: Optional[str] = None):
        """
        Initialize PubMed search client.

        Args:
            email: Your email (required by NCBI for contact if issues arise)
            api_key: Optional NCBI API key (increases rate limit from 3 to 10 requests/sec)
        """
        self.base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
        self.email = email
        self.api_key = api_key

        # Rate limiting
        self.requests_per_second = 10 if api_key else 3
        self.min_request_interval = 1.0 / self.requests_per_second
        self.last_request_time = 0

    async def _rate_limit(self):
        """Ensure we don't exceed rate limits"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.min_request_interval:
            await asyncio.sleep(self.min_request_interval - time_since_last)
        self.last_request_time = time.time()

    async def search_author(
        self, session: aiohttp.ClientSession, lastname: str, firstname: str
    ) -> List[str]:
        """
        Search for PMIDs associated with an author.

        Returns list of PMIDs (publication IDs)
        """
        await self._rate_limit()

        # Build search query - try full name and first initial
        queries = [
            f"{lastname} {firstname}[Author]",
            (
                f"{lastname} {firstname[0]}[Author]"
                if firstname
                else f"{lastname}[Author]"
            ),
        ]

        all_pmids = set()

        for query in queries:
            params = {
                "db": "pubmed",
                "term": query,
                "retmax": 20,  # Limit to most recent 20 papers
                "sort": "date",
                "email": self.email,
                "retmode": "json",
            }
            if self.api_key:
                params["api_key"] = self.api_key

            url = f"{self.base_url}/esearch.fcgi"

            try:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        if (
                            "esearchresult" in data
                            and "idlist" in data["esearchresult"]
                        ):
                            all_pmids.update(data["esearchresult"]["idlist"])
            except Exception as e:
                print(f"Error searching for {firstname} {lastname}: {e}")

        return list(all_pmids)[:10]  # Return top 10 PMIDs

    async def fetch_article_details(
        self, session: aiohttp.ClientSession, pmids: List[str]
    ) -> List[Dict]:
        """
        Fetch detailed information for given PMIDs, including author affiliations.
        """
        if not pmids:
            return []

        await self._rate_limit()

        params = {
            "db": "pubmed",
            "id": ",".join(pmids),
            "retmode": "xml",
            "email": self.email,
        }
        if self.api_key:
            params["api_key"] = self.api_key

        url = f"{self.base_url}/efetch.fcgi"

        try:
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    xml_data = await response.text()
                    return self._parse_article_xml(xml_data)
        except Exception as e:
            print(f"Error fetching article details: {e}")

        return []

    def _parse_article_xml(self, xml_data: str) -> List[Dict]:
        """Parse PubMed XML to extract author and affiliation information."""
        articles = []

        try:
            root = ET.fromstring(xml_data)

            for article in root.findall(".//PubmedArticle"):
                article_data = {"pmid": "", "title": "", "authors": [], "year": ""}

                # Get PMID
                pmid_elem = article.find(".//PMID")
                if pmid_elem is not None:
                    article_data["pmid"] = pmid_elem.text

                # Get title
                title_elem = article.find(".//ArticleTitle")
                if title_elem is not None:
                    article_data["title"] = title_elem.text

                # Get publication year
                year_elem = article.find(".//PubDate/Year")
                if year_elem is not None:
                    article_data["year"] = year_elem.text

                # Get authors with affiliations
                author_list = article.find(".//AuthorList")
                if author_list is not None:
                    for author in author_list.findall("Author"):
                        author_info = {}

                        # Get name
                        lastname = author.find("LastName")
                        firstname = author.find("ForeName")
                        if lastname is not None:
                            author_info["lastname"] = lastname.text
                        if firstname is not None:
                            author_info["firstname"] = firstname.text

                        # Get affiliation
                        affiliation = author.find(".//Affiliation")
                        if affiliation is not None:
                            author_info["affiliation"] = affiliation.text

                            # Try to extract email if present
                            import re

                            email_match = re.search(
                                r"[\w\.-]+@[\w\.-]+\.\w+", affiliation.text
                            )
                            if email_match:
                                author_info["email"] = email_match.group()

                        # Get ORCID if available
                        for identifier in author.findall(".//Identifier"):
                            if identifier.get("Source") == "ORCID":
                                author_info["orcid"] = identifier.text

                        article_data["authors"].append(author_info)

                articles.append(article_data)

        except ET.ParseError as e:
            print(f"Error parsing XML: {e}")

        return articles

    async def find_author_affiliations(self, lastname: str, firstname: str) -> Dict:
        """
        Main method to find author affiliations.

        Returns dict with author info and their affiliations from recent papers.
        """
        async with aiohttp.ClientSession() as session:
            # Search for PMIDs
            pmids = await self.search_author(session, lastname, firstname)

            if not pmids:
                return {
                    "query": f"{firstname} {lastname}",
                    "found": False,
                    "affiliations": [],
                }

            # Fetch article details
            articles = await self.fetch_article_details(session, pmids)

            # Extract unique affiliations for this author
            affiliations = {}
            emails = set()
            orcids = set()

            for article in articles:
                for author in article["authors"]:
                    # Match the author we're looking for (case-insensitive)
                    lastname_match = (
                        author.get("lastname", "").lower() == lastname.lower()
                    )
                    firstname_match = False

                    if firstname and author.get("firstname"):
                        # Check if first initial matches or full firstname matches
                        firstname_match = (
                            author.get("firstname", "")
                            .lower()
                            .startswith(firstname[0].lower())
                            or author.get("firstname", "").lower() == firstname.lower()
                        )

                    if lastname_match and (not firstname or firstname_match):
                        if "affiliation" in author and author["affiliation"]:
                            # Use affiliation as key to track years
                            aff = author["affiliation"]
                            if aff not in affiliations:
                                affiliations[aff] = {"years": set(), "pmids": []}
                            if article["year"]:
                                affiliations[aff]["years"].add(article["year"])
                            affiliations[aff]["pmids"].append(article["pmid"])

                        if "email" in author:
                            emails.add(author["email"])
                        if "orcid" in author:
                            orcids.add(author["orcid"])

            # Format results
            result = {
                "query": f"{firstname} {lastname}",
                "found": len(affiliations) > 0,
                "num_papers_checked": len(articles),
                "affiliations": [],
            }

            for aff_text, aff_data in affiliations.items():
                result["affiliations"].append(
                    {
                        "text": aff_text,
                        "years": sorted(list(aff_data["years"])),
                        "num_papers": len(aff_data["pmids"]),
                        "pmids": aff_data["pmids"][:3],  # Sample PMIDs
                    }
                )

            if emails:
                result["emails"] = list(emails)
            if orcids:
                result["orcids"] = list(orcids)

            return result


def read_authors_from_csv(filename: str) -> List[tuple]:
    """
    Read authors from CSV file.

    Expects columns: Lastname, Firstname (case-insensitive)
    Returns list of (lastname, firstname) tuples
    """
    authors = []

    try:
        with open(filename, "r", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)

            # Get column names (case-insensitive matching)
            fieldnames = reader.fieldnames
            lastname_col = None
            firstname_col = None

            for field in fieldnames:
                field_lower = field.lower().strip()
                if "lastname" in field_lower or "last_name" in field_lower:
                    lastname_col = field
                elif "firstname" in field_lower or "first_name" in field_lower:
                    firstname_col = field

            if not lastname_col:
                print("Error: Could not find Lastname column in CSV")
                return authors
            if not firstname_col:
                print("Error: Could not find Firstname column in CSV")
                return authors

            print(
                f"Using columns: Lastname='{lastname_col}', Firstname='{firstname_col}'"
            )

            for row in reader:
                lastname = row.get(lastname_col, "").strip()
                firstname = row.get(firstname_col, "").strip()

                if lastname:  # At minimum we need a last name
                    authors.append((lastname, firstname))

        print(f"Loaded {len(authors)} authors from {filename}")

    except FileNotFoundError:
        print(f"Error: File '{filename}' not found")
    except Exception as e:
        print(f"Error reading CSV file: {e}")

    return authors


async def search_multiple_authors(
    authors: List[tuple], email: str, api_key: Optional[str] = None
):
    """
    Search for multiple authors concurrently.

    Args:
        authors: List of (lastname, firstname) tuples
        email: Your email for NCBI
        api_key: Optional API key for higher rate limits
    """
    searcher = PubMedAuthorSearch(email, api_key)

    # Process in batches to respect rate limits
    batch_size = 5
    results = []
    total_authors = len(authors)

    print(f"\nSearching PubMed for {total_authors} authors...")
    print("=" * 60)

    for i in range(0, len(authors), batch_size):
        batch = authors[i : i + batch_size]
        batch_num = (i // batch_size) + 1
        total_batches = (total_authors + batch_size - 1) // batch_size

        print(
            f"Processing batch {batch_num}/{total_batches} ({i+1}-{min(i+batch_size, total_authors)} of {total_authors})"
        )

        # Use list(starmap(function, iterable)) instead of list comprehensions 
        # where the function arguments match the tuple unpacking pattern
        tasks = list(starmap(searcher.find_author_affiliations, batch))
        batch_results = await asyncio.gather(*tasks)
        results.extend(batch_results)

        # Small delay between batches
        if i + batch_size < len(authors):
            await asyncio.sleep(1)

    return results


def save_results(results: List[Dict], output_file: str = None):
    """Save results to JSON and/or CSV file"""
    if output_file is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"pubmed_results_{timestamp}"

    # Save as JSON for complete data
    json_file = f"{output_file}.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\nComplete results saved to: {json_file}")

    # Save simplified CSV for easy viewing
    csv_file = f"{output_file}.csv"
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "Query",
                "Found",
                "Num_Papers",
                "Emails",
                "ORCIDs",
                "Most_Recent_Affiliation",
                "Years",
            ]
        )

        for result in results:
            most_recent_aff = ""
            years = ""
            if result["affiliations"]:
                # Get most recent affiliation
                most_recent = max(
                    result["affiliations"],
                    key=lambda x: max(x["years"]) if x["years"] else "0",
                )
                most_recent_aff = most_recent["text"][
                    :200
                ]  # Truncate long affiliations
                years = ", ".join(most_recent["years"])

            emails = ", ".join(result.get("emails", []))
            orcids = ", ".join(result.get("orcids", []))

            writer.writerow(
                [
                    result["query"],
                    result["found"],
                    result.get("num_papers_checked", 0),
                    emails,
                    orcids,
                    most_recent_aff,
                    years,
                ]
            )

    print(f"Summary CSV saved to: {csv_file}")


async def main():
    # TODO: Configuration
    EMAIL = "your.email@example.com"  # REPLACE with your email
    API_KEY = "your_api_key_here"  # REPLACE with your NCBI API key
    CSV_FILE = "not_found.csv"

    # Read authors from CSV
    authors_to_search = read_authors_from_csv(CSV_FILE)

    if not authors_to_search:
        print("No authors to search. Exiting.")
        return

    # Search PubMed
    results = await search_multiple_authors(authors_to_search, EMAIL, API_KEY)

    # Display results
    print("\n" + "=" * 60)
    print("RESULTS SUMMARY")
    print("=" * 60)

    found_count = 0
    for result in results:
        if result["found"]:
            found_count += 1
            print(f"\n✓ {result['query']}")
            print(f"  Papers checked: {result['num_papers_checked']}")

            if "emails" in result:
                print(f"  Emails: {', '.join(result['emails'])}")
            if "orcids" in result:
                print(f"  ORCIDs: {', '.join(result['orcids'])}")

            if result["affiliations"]:
                print("  Most recent affiliation:")
                most_recent = result["affiliations"][0]
                print(f"    {most_recent['text'][:150]}...")
                print(f"    Years: {', '.join(most_recent['years'])}")
        else:
            print(f"\n✗ {result['query']} - No publications found")

    print(f"\n{'-' * 60}")
    print(f"Total authors searched: {len(results)}")
    print(f"Found in PubMed: {found_count}")
    print(f"Not found: {len(results) - found_count}")

    # Save results
    save_results(results)


if __name__ == "__main__":
    # IMPORTANT: Update EMAIL and API_KEY before running!
    asyncio.run(main())

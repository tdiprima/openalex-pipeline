"""
Quickly get total number of Stony Brook authors in OpenAlex
using the count from API metadata (single request).
"""

import asyncio
import os

import aiohttp
from dotenv import load_dotenv

load_dotenv()


async def count_all_authors():
    """Get count of all Stony Brook authors in OpenAlex"""

    BASE_URL = "https://api.openalex.org"
    STONYBROOK_ROR = "05qghxh33"
    email = os.getenv("OPENALEX_EMAIL")

    print("🔍 Checking total Stony Brook authors in OpenAlex...")
    print(f"Using ROR: {STONYBROOK_ROR}\n")

    async with aiohttp.ClientSession() as session:
        url = f"{BASE_URL}/authors"
        params = {
            "filter": f"affiliations.institution.ror:{STONYBROOK_ROR}",
            "per-page": 1,  # We only need the metadata, not the results
            "mailto": email,
        }

        async with session.get(url, params=params) as resp:
            if resp.status != 200:
                print(f"Error: HTTP {resp.status}")
                return None

            data = await resp.json()
            meta = data.get("meta", {})
            total_count = meta.get("count", 0)

            print("=" * 60)
            print(f"✅ TOTAL STONY BROOK AUTHORS IN OPENALEX: {total_count:,}")
            print("=" * 60)
            print(
                f"\nYou can now run openalex_pipeline.py with max_authors={total_count}"
            )

            return total_count


if __name__ == "__main__":
    asyncio.run(count_all_authors())

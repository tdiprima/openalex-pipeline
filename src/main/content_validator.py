"""
Content validation module for checking Stony Brook-related content.
"""

import re
from typing import Any, Dict


class ContentValidator:
    """Validates and checks content for specific patterns and mentions"""

    def __init__(self):
        self.stonybrook_patterns = [
            r"stony\s*brook",
            r"suny\s*stony\s*brook",
            r"state\s*university\s*of\s*new\s*york\s*at\s*stony\s*brook",
            r"stony\s*brook\s*university",
            r"stony\s*brook\s*medicine",
            r"sbu\b",  # Common abbreviation
        ]

    def check_stonybrook_content(self, text: str) -> Dict[str, Any]:
        """
        Check if text contains Stony Brook-related content.

        Args:
            text: Text to check

        Returns:
            dict with:
                - found: bool
                - mentions: list of matched phrases
                - context: list of sentences containing matches
                - count: total number of mentions
        """
        mentions = []
        contexts = []

        text_lower = text.lower()

        for pattern in self.stonybrook_patterns:
            matches = re.finditer(pattern, text_lower, re.IGNORECASE)
            for match in matches:
                mentions.append(match.group())

                # Extract surrounding context (100 chars before and after)
                start = max(0, match.start() - 100)
                end = min(len(text), match.end() + 100)
                context = text[start:end].replace("\n", " ")
                contexts.append(f"...{context}...")

        return {
            "found": len(mentions) > 0,
            "mentions": list(set(mentions)),
            "contexts": contexts[:3],  # Limit to 3 examples
            "count": len(mentions),
        }

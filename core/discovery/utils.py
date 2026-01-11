"""
Shared utility functions for the data source discovery workflow.

This module provides common utilities used across multiple agents.
"""

import json
import logging
import re
from typing import Dict, Any, Optional, List, Tuple
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


def sanitize_for_format_string(text: str) -> str:
    """
    Sanitize text for safe use in Python format strings.

    Web content and user input may contain curly braces that would be
    interpreted as format placeholders, causing KeyError exceptions.
    This function escapes curly braces by doubling them.

    Args:
        text: The input text that may contain curly braces

    Returns:
        Text with curly braces escaped ({{ and }})
    """
    if not text or not isinstance(text, str):
        return text or ""

    # Escape curly braces by doubling them
    return text.replace("{", "{{").replace("}", "}}")


def extract_first_json_object(content: str) -> Optional[Dict[str, Any]]:
    """
    Extract the first valid JSON object from a string.

    This handles cases where the LLM returns multiple JSON objects or
    extra text after the JSON, which would cause json.loads() to fail
    with "Extra data" error.

    Args:
        content: String that may contain one or more JSON objects

    Returns:
        The first parsed JSON object, or None if no valid JSON found
    """
    start = content.find("{")
    if start == -1:
        logger.error(
            f"JSON parsing failed: No '{{' found in LLM response.\n"
            f"=== OFFENDING LLM RESPONSE (first 2000 chars) ===\n"
            f"{content[:2000]}\n"
            f"=== END OFFENDING RESPONSE ==="
        )
        return None

    # Use raw_decode to parse only the first complete JSON object
    decoder = json.JSONDecoder()
    try:
        obj, _ = decoder.raw_decode(content[start:])
        return obj
    except json.JSONDecodeError as e:
        logger.error(
            f"JSON parsing failed: {e}\n"
            f"=== OFFENDING LLM RESPONSE (first 2000 chars) ===\n"
            f"{content[:2000]}\n"
            f"=== END OFFENDING RESPONSE ==="
        )
        return None


def extract_first_json_array(content: str) -> Optional[List[Any]]:
    """
    Extract the first valid JSON array from a string.

    This handles cases where the LLM returns multiple JSON arrays or
    extra text after the JSON, which would cause json.loads() to fail
    with "Extra data" error.

    Args:
        content: String that may contain one or more JSON arrays

    Returns:
        The first parsed JSON array, or None if no valid JSON found
    """
    start = content.find("[")
    if start == -1:
        logger.error(
            f"JSON array parsing failed: No '[' found in LLM response.\n"
            f"=== OFFENDING LLM RESPONSE (first 2000 chars) ===\n"
            f"{content[:2000]}\n"
            f"=== END OFFENDING RESPONSE ==="
        )
        return None

    # Use raw_decode to parse only the first complete JSON array
    decoder = json.JSONDecoder()
    try:
        arr, _ = decoder.raw_decode(content[start:])
        return arr
    except json.JSONDecodeError as e:
        logger.error(
            f"JSON array parsing failed: {e}\n"
            f"=== OFFENDING LLM RESPONSE (first 2000 chars) ===\n"
            f"{content[:2000]}\n"
            f"=== END OFFENDING RESPONSE ==="
        )
        return None


def check_relevance_from_metadata(
    source_name: str,
    source_url: str,
    source_description: str,
    user_description: str,
) -> Tuple[bool, str]:
    """
    Determine if a source is relevant based on metadata alone.

    This function checks relevance without fetching the page, making it
    resilient to network failures. Since search engines already filtered
    results for the user's query, we're generous here - only marking
    sources as irrelevant if they're clearly unrelated.

    Args:
        source_name: Name of the data source
        source_url: URL of the data source
        source_description: Description from search results
        user_description: What the user is looking for

    Returns:
        Tuple of (is_relevant, reason)
    """
    # Normalize everything to lowercase for comparison
    name_lower = (source_name or "").lower()
    url_lower = (source_url or "").lower()
    desc_lower = (source_description or "").lower()
    user_lower = (user_description or "").lower()

    # Extract key terms from user description (words 3+ chars, excluding common words)
    stop_words = {
        "the", "and", "for", "with", "that", "this", "from", "are", "was",
        "were", "been", "have", "has", "had", "will", "would", "could", "should",
        "can", "may", "might", "data", "source", "api", "get", "find", "need",
        "want", "looking", "search", "about", "information", "info", "access"
    }
    user_terms = set(
        word for word in re.findall(r'\b[a-z]+\b', user_lower)
        if len(word) >= 3 and word not in stop_words
    )

    # Combine all source metadata
    source_text = f"{name_lower} {url_lower} {desc_lower}"

    # Check for term matches
    matching_terms = [term for term in user_terms if term in source_text]

    # Domain authority indicators (bonus relevance)
    parsed_url = urlparse(source_url or "")
    domain = parsed_url.netloc.lower()

    authority_domains = [".gov", ".edu", ".org", "api.", "data.", "developer."]
    is_authority = any(indicator in domain for indicator in authority_domains)

    # Decision logic - be GENEROUS (only reject clearly irrelevant)
    if matching_terms:
        return (True, f"Metadata contains relevant terms: {', '.join(matching_terms[:5])}")

    if is_authority:
        # Authority domains get benefit of the doubt even without term matches
        return (True, f"Authority domain ({domain}) - assuming relevant")

    # If the source has a non-empty description, assume it's relevant
    # since it was found by a search for the user's query
    if desc_lower and len(desc_lower) > 20:
        return (True, "Search engine returned this result - assuming relevant")

    # If we have almost no metadata, still be generous
    if not name_lower and not desc_lower:
        return (True, "Insufficient metadata to determine - assuming relevant")

    # Only mark as irrelevant if we have metadata but no overlap at all
    return (True, "Default to relevant - search engine pre-filtered results")

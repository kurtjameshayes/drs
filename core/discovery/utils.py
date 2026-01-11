"""
Shared utility functions for the data source discovery workflow.

This module provides common utilities used across multiple agents.
"""

import json
import logging
from typing import Dict, Any, Optional, List

logger = logging.getLogger(__name__)


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

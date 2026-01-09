"""
LLM Logging Utilities for the data source discovery workflow.

This module provides utilities for logging LLM prompts and responses,
enabling visibility into agent-LLM interactions for debugging and monitoring.
"""

import logging
import time
from typing import Any, List, Optional

from langchain_core.messages import BaseMessage

try:
    from anthropic import BadRequestError
except ImportError:
    # Fallback if anthropic is not installed
    BadRequestError = Exception

logger = logging.getLogger(__name__)


def format_messages_for_logging(messages: List[BaseMessage], max_length: int = 1000) -> str:
    """
    Format LLM messages for logging output.

    Args:
        messages: List of LangChain messages (SystemMessage, HumanMessage, etc.)
        max_length: Maximum length for each message content in the log

    Returns:
        Formatted string representation of messages
    """
    formatted_parts = []
    for i, msg in enumerate(messages):
        msg_type = msg.__class__.__name__
        content = msg.content
        if len(content) > max_length:
            content = content[:max_length] + f"... [truncated, {len(msg.content)} chars total]"
        formatted_parts.append(f"  [{i}] {msg_type}:\n    {content}")
    return "\n".join(formatted_parts)


def format_response_for_logging(response: Any, max_length: int = 2000) -> str:
    """
    Format LLM response for logging output.

    Args:
        response: LLM response object
        max_length: Maximum length for content in the log

    Returns:
        Formatted string representation of response
    """
    content = getattr(response, 'content', str(response))
    if len(content) > max_length:
        content = content[:max_length] + f"... [truncated, {len(response.content)} chars total]"
    return content


def invoke_llm_with_logging(
    llm: Any,
    messages: List[BaseMessage],
    agent_name: str,
    operation: Optional[str] = None,
    log_level: int = logging.DEBUG
) -> Any:
    """
    Invoke an LLM with logging of the prompt and response.

    This function wraps the LLM invoke call to provide visibility into
    the prompts being sent and responses received.

    Args:
        llm: The LangChain LLM instance (e.g., ChatAnthropic)
        messages: List of messages to send to the LLM
        agent_name: Name of the calling agent for log context
        operation: Optional description of the operation being performed
        log_level: Logging level to use (default: DEBUG)

    Returns:
        The LLM response object

    Example:
        response = invoke_llm_with_logging(
            self.llm,
            messages,
            agent_name="SearchAgent",
            operation="generate_search_queries"
        )
    """
    op_desc = f" ({operation})" if operation else ""

    # Log the prompt
    logger.log(
        log_level,
        f"[{agent_name}]{op_desc} LLM Request:\n"
        f"  Model: {getattr(llm, 'model', 'unknown')}\n"
        f"  Messages:\n{format_messages_for_logging(messages)}"
    )

    # Track timing
    start_time = time.time()

    try:
        # Invoke the LLM
        response = llm.invoke(messages)
    except BadRequestError as e:
        # Handle Anthropic API errors, especially content filtering
        error_message = str(e)

        if "content filtering" in error_message.lower():
            logger.error(
                f"[{agent_name}]{op_desc} Content Filtering Error:\n"
                f"  The LLM response was blocked by Anthropic's content filtering policy.\n"
                f"  This may be due to:\n"
                f"  - Sensitive content in the documentation being analyzed\n"
                f"  - Potentially harmful patterns in the API response examples\n"
                f"  - Personal information or credentials in the source material\n"
                f"  Error: {error_message}"
            )
            # Re-raise with a more informative message
            raise ValueError(
                f"Content filtering blocked the response for {agent_name}. "
                f"The source documentation may contain sensitive content that triggers "
                f"Anthropic's safety policies. Consider reviewing the source material "
                f"or contact support if this is unexpected."
            ) from e
        else:
            # For other BadRequestErrors, log and re-raise
            logger.error(f"[{agent_name}]{op_desc} API Error: {error_message}")
            raise
    except Exception as e:
        # Log any other unexpected errors
        logger.error(f"[{agent_name}]{op_desc} Unexpected error during LLM invocation: {e}", exc_info=True)
        raise

    # Calculate elapsed time
    elapsed_ms = (time.time() - start_time) * 1000

    # Log the response
    logger.log(
        log_level,
        f"[{agent_name}]{op_desc} LLM Response ({elapsed_ms:.0f}ms):\n"
        f"  Content:\n    {format_response_for_logging(response)}"
    )

    return response

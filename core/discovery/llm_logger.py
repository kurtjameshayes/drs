"""
LLM Logging Utilities for the data source discovery workflow.

This module provides utilities for logging LLM prompts and responses,
enabling visibility into agent-LLM interactions for debugging and monitoring.
"""

import json
import logging
import time
from typing import Any, Dict, List, Optional, Union
from functools import wraps

from langchain_core.messages import BaseMessage

try:
    from anthropic import BadRequestError
except ImportError:
    # Fallback if anthropic is not installed
    BadRequestError = Exception

logger = logging.getLogger(__name__)


# =============================================================================
# Comprehensive Logging Utilities for Agent Steps
# =============================================================================

class AgentLogger:
    """
    Comprehensive logging utility for agent operations.

    Provides structured logging at each step showing:
    - Input variables
    - Prompts being sent to LLM
    - LLM reasoning/response
    - Results and outcomes
    """

    def __init__(self, agent_name: str, log_level: int = logging.INFO):
        """
        Initialize the agent logger.

        Args:
            agent_name: Name of the agent (e.g., "APIKeyAgent")
            log_level: Default logging level for step logs
        """
        self.agent_name = agent_name
        self.log_level = log_level
        self.step_counter = 0
        self._logger = logging.getLogger(f"{__name__}.{agent_name}")

    def _format_value(self, value: Any, max_length: int = 500) -> str:
        """Format a value for logging, truncating if needed."""
        if value is None:
            return "None"
        if isinstance(value, dict):
            try:
                formatted = json.dumps(value, indent=2, default=str)
            except (TypeError, ValueError):
                formatted = str(value)
        elif isinstance(value, (list, tuple)):
            try:
                formatted = json.dumps(value, indent=2, default=str)
            except (TypeError, ValueError):
                formatted = str(value)
        else:
            formatted = str(value)

        if len(formatted) > max_length:
            return formatted[:max_length] + f"... [truncated, {len(formatted)} chars total]"
        return formatted

    def _format_variables(self, variables: Dict[str, Any], indent: str = "    ") -> str:
        """Format a dict of variables for logging."""
        if not variables:
            return f"{indent}(none)"

        lines = []
        for key, value in variables.items():
            formatted_value = self._format_value(value)
            # Handle multi-line values
            if "\n" in formatted_value:
                lines.append(f"{indent}{key}:")
                for line in formatted_value.split("\n"):
                    lines.append(f"{indent}  {line}")
            else:
                lines.append(f"{indent}{key}: {formatted_value}")
        return "\n".join(lines)

    def log_step_start(
        self,
        step_name: str,
        step_description: str,
        variables: Optional[Dict[str, Any]] = None,
    ) -> int:
        """
        Log the start of an agent step with input variables.

        Args:
            step_name: Short identifier for the step (e.g., "discover_site")
            step_description: Human-readable description of what this step does
            variables: Dictionary of input variables to log

        Returns:
            Step number for reference in subsequent logs
        """
        self.step_counter += 1
        step_num = self.step_counter

        divider = "=" * 70
        self._logger.log(
            self.log_level,
            f"\n{divider}\n"
            f"[{self.agent_name}] STEP {step_num}: {step_name}\n"
            f"{divider}\n"
            f"Description: {step_description}\n"
            f"Input Variables:\n{self._format_variables(variables or {})}"
        )

        return step_num

    def log_prompt(
        self,
        step_num: int,
        operation: str,
        prompt: str,
        additional_context: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Log the prompt being sent to the LLM.

        Args:
            step_num: Step number from log_step_start
            operation: Name of the LLM operation
            prompt: The full prompt text
            additional_context: Any additional context being provided
        """
        context_str = ""
        if additional_context:
            context_str = f"\nAdditional Context:\n{self._format_variables(additional_context)}"

        prompt_preview = prompt if len(prompt) <= 2000 else prompt[:2000] + f"... [truncated, {len(prompt)} chars total]"

        self._logger.log(
            self.log_level,
            f"\n[{self.agent_name}] Step {step_num} - LLM Prompt ({operation}):\n"
            f"{'─' * 50}\n"
            f"{prompt_preview}"
            f"{context_str}\n"
            f"{'─' * 50}"
        )

    def log_llm_response(
        self,
        step_num: int,
        operation: str,
        response_content: str,
        parsed_result: Optional[Any] = None,
        elapsed_ms: Optional[float] = None,
    ) -> None:
        """
        Log the LLM response and reasoning.

        Args:
            step_num: Step number from log_step_start
            operation: Name of the LLM operation
            response_content: Raw response content from LLM
            parsed_result: Parsed/structured result if applicable
            elapsed_ms: Time taken for LLM call in milliseconds
        """
        time_str = f" ({elapsed_ms:.0f}ms)" if elapsed_ms else ""

        response_preview = response_content if len(response_content) <= 2000 else response_content[:2000] + f"... [truncated, {len(response_content)} chars total]"

        parsed_str = ""
        if parsed_result is not None:
            parsed_str = f"\nParsed Result:\n{self._format_variables({'result': parsed_result})}"

        self._logger.log(
            self.log_level,
            f"\n[{self.agent_name}] Step {step_num} - LLM Response ({operation}){time_str}:\n"
            f"{'─' * 50}\n"
            f"Raw Response:\n{response_preview}"
            f"{parsed_str}\n"
            f"{'─' * 50}"
        )

    def log_step_result(
        self,
        step_num: int,
        step_name: str,
        success: bool,
        result: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
    ) -> None:
        """
        Log the result/outcome of a step.

        Args:
            step_num: Step number from log_step_start
            step_name: Name of the step
            success: Whether the step succeeded
            result: Result data if successful
            error: Error message if failed
        """
        status = "✓ SUCCESS" if success else "✗ FAILED"

        result_str = ""
        if result:
            result_str = f"\nResult:\n{self._format_variables(result)}"

        error_str = ""
        if error:
            error_str = f"\nError: {error}"

        self._logger.log(
            self.log_level,
            f"\n[{self.agent_name}] Step {step_num} ({step_name}) - {status}"
            f"{result_str}"
            f"{error_str}\n"
            f"{'=' * 70}"
        )

    def log_decision(
        self,
        step_num: int,
        decision_point: str,
        condition: str,
        result: bool,
        action: str,
    ) -> None:
        """
        Log a decision point in the agent logic.

        Args:
            step_num: Step number from log_step_start
            decision_point: Name of the decision point
            condition: The condition being evaluated
            result: Result of the condition (True/False)
            action: Action being taken based on the decision
        """
        self._logger.log(
            self.log_level,
            f"[{self.agent_name}] Step {step_num} - Decision: {decision_point}\n"
            f"    Condition: {condition}\n"
            f"    Evaluation: {result}\n"
            f"    Action: {action}"
        )

    def log_substep(
        self,
        step_num: int,
        substep: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Log a substep within a larger step.

        Args:
            step_num: Parent step number
            substep: Description of the substep
            details: Optional details about the substep
        """
        details_str = ""
        if details:
            details_str = f"\n{self._format_variables(details)}"

        self._logger.log(
            self.log_level,
            f"[{self.agent_name}] Step {step_num} - Substep: {substep}{details_str}"
        )

    def log_info(self, message: str, **kwargs: Any) -> None:
        """Log an info message with optional key-value pairs."""
        if kwargs:
            details = "\n" + self._format_variables(kwargs)
        else:
            details = ""
        self._logger.info(f"[{self.agent_name}] {message}{details}")

    def log_warning(self, message: str, **kwargs: Any) -> None:
        """Log a warning message with optional key-value pairs."""
        if kwargs:
            details = "\n" + self._format_variables(kwargs)
        else:
            details = ""
        self._logger.warning(f"[{self.agent_name}] {message}{details}")

    def log_error(self, message: str, error: Optional[Exception] = None, **kwargs: Any) -> None:
        """Log an error message with optional exception details."""
        if kwargs:
            details = "\n" + self._format_variables(kwargs)
        else:
            details = ""
        if error:
            self._logger.error(f"[{self.agent_name}] {message}{details}", exc_info=True)
        else:
            self._logger.error(f"[{self.agent_name}] {message}{details}")


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

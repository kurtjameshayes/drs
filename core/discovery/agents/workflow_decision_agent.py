"""
Workflow Decision Agent

Agent that makes intelligent decisions about workflow routing using LLM-based
reasoning with rule-based safety checks. Analyzes errors and recommends
recovery strategies to maximize autonomous operation.
"""

from typing import Dict, Any, Literal, Optional
from dataclasses import dataclass
import json
import logging
from datetime import datetime
from langchain_anthropic import ChatAnthropic
from langchain_core.messages import SystemMessage, HumanMessage

from config import Config
from core.discovery.llm_logger import invoke_llm_with_logging

logger = logging.getLogger(__name__)


@dataclass
class RecoveryDecision:
    """Structured decision from the WorkflowDecisionAgent."""
    action: Literal[
        "needs_key",      # Route to API key acquisition
        "retry_test",     # Retry the test step
        "skip_source",    # Skip to next candidate
        "pause_manual",   # Pause for manual user input
        "configure"       # Proceed to configuration (error is minor)
    ]
    reasoning: str
    confidence: float  # 0.0 - 1.0
    user_message: Optional[str] = None  # For pause_manual
    retry_recommended: bool = False
    max_retries_suggestion: int = 3


class WorkflowDecisionAgent:
    """
    Agent that makes intelligent decisions about workflow routing
    using LLM-based reasoning with rule-based safety checks.

    This agent analyzes errors during workflow execution and recommends
    the best recovery strategy, prioritizing autonomous recovery over
    manual intervention.
    """

    def __init__(self):
        self.llm = ChatAnthropic(
            model=Config.DISCOVERY_LLM_MODEL,
            api_key=Config.ANTHROPIC_API_KEY,
            temperature=0.1,  # Low temperature for consistent decisions
        )

    def analyze_error_recovery(
        self,
        error_context: Dict[str, Any],
        workflow_state: Dict[str, Any]
    ) -> RecoveryDecision:
        """
        Analyze an error and recommend recovery strategy.

        Args:
            error_context: Details about the error
                - issue: Error message/description
                - details: Additional error details
                - status_code: HTTP status code (if applicable)
                - agent_name: Which agent encountered the error
            workflow_state: Current state context
                - current_step: Which step failed
                - test_attempts: Number of test attempts so far
                - api_key_present: Whether API key is configured
                - api_key_acquisition_attempted: Whether we tried to get key

        Returns:
            RecoveryDecision with recommended action and reasoning
        """
        try:
            prompt = self._build_error_recovery_prompt(error_context, workflow_state)

            messages = [
                SystemMessage(content=self._get_system_prompt()),
                HumanMessage(content=prompt)
            ]

            response = invoke_llm_with_logging(
                llm=self.llm,
                messages=messages,
                agent_name="WorkflowDecisionAgent",
                decision_type="error_recovery"
            )

            # Parse JSON response
            decision_data = self._parse_llm_response(response.content)

            # Validate and construct RecoveryDecision
            return self._validate_decision(decision_data, error_context, workflow_state)

        except Exception as e:
            logger.error(f"WorkflowDecisionAgent failed: {e}", exc_info=True)
            # Fallback to conservative decision
            return self._fallback_decision(error_context, workflow_state)

    def should_pause_for_human(
        self,
        error_context: Dict[str, Any],
        workflow_state: Dict[str, Any],
        recovery_decision: RecoveryDecision
    ) -> tuple[bool, Optional[str]]:
        """
        Determine if workflow should pause for human input.

        Returns:
            (should_pause, reason_message)
        """
        # If LLM explicitly recommended pause
        if recovery_decision.action == "pause_manual":
            return True, recovery_decision.user_message

        # Check confidence threshold
        if recovery_decision.confidence < 0.5:
            return True, "Low confidence in automatic recovery. Manual review recommended."

        # Check if we've exhausted automatic options
        attempts = workflow_state.get("test_attempts", 0)
        if attempts >= recovery_decision.max_retries_suggestion:
            return True, f"Maximum retry attempts ({attempts}) reached."

        return False, None

    def _get_system_prompt(self) -> str:
        """System prompt defining the agent's role and capabilities."""
        return """You are a workflow orchestration decision agent for an API discovery system.

Your role is to analyze errors during the discovery workflow and recommend the best recovery strategy.

The workflow steps are:
1. search - Find data source candidates
2. examine - Analyze access methods
3. select - Choose best source
4. document - Extract API documentation
5. test - Test API endpoints
6. configure - Generate MongoDB Atlas connector config

When an error occurs, you must recommend ONE of these actions:
- "needs_key": The error is due to missing/invalid authentication. Route to API key acquisition.
- "retry_test": The error is temporary or transient. Retry the same step.
- "skip_source": The error is permanent for this source. Skip to next candidate.
- "pause_manual": The error requires human judgment. Pause for manual input.
- "configure": The error is minor/recoverable. Proceed to next step.

IMPORTANT GUIDELINES:
1. Prioritize autonomous recovery - only pause for manual input when truly necessary
2. Distinguish between temporary failures (network, rate limit) vs permanent (invalid URL, no data)
3. Authentication errors (401, 403, "unauthorized", "forbidden") usually need "needs_key"
4. Network errors (timeout, connection refused) usually need "retry_test"
5. Data format issues may be recoverable with "configure"
6. Be conservative with "skip_source" - only if clearly no path forward

Respond ONLY with valid JSON in this exact format:
{
  "action": "<one of: needs_key, retry_test, skip_source, pause_manual, configure>",
  "reasoning": "<detailed explanation of why this action is best>",
  "confidence": <float 0.0-1.0>,
  "user_message": "<if action=pause_manual, explain what user should do>",
  "retry_recommended": <boolean>,
  "max_retries_suggestion": <integer 1-5>
}
"""

    def _build_error_recovery_prompt(
        self,
        error_context: Dict[str, Any],
        workflow_state: Dict[str, Any]
    ) -> str:
        """Build the specific prompt for this error."""
        return f"""Analyze this error and recommend a recovery strategy:

## Error Context
- Issue: {error_context.get('issue', 'Unknown')}
- Details: {error_context.get('details', 'No details provided')}
- HTTP Status Code: {error_context.get('status_code', 'N/A')}
- Agent: {error_context.get('agent_name', 'Unknown')}

## Workflow State
- Current Step: {workflow_state.get('current_step', 'Unknown')}
- Test Attempts So Far: {workflow_state.get('test_attempts', 0)}
- API Key Present: {workflow_state.get('api_key_present', False)}
- API Key Acquisition Attempted: {workflow_state.get('api_key_acquisition_attempted', False)}
- Selected Source: {workflow_state.get('selected_source_name', 'Unknown')}

## Previous Actions
{self._format_action_history(workflow_state.get('action_history', []))}

Based on this context, what is the best recovery strategy?
"""

    def _format_action_history(self, history: list) -> str:
        """Format action history for prompt."""
        if not history:
            return "No previous actions"
        return "\n".join([f"- {action}" for action in history[-5:]])  # Last 5 actions

    def _parse_llm_response(self, response_text: str) -> Dict[str, Any]:
        """Parse JSON from LLM response."""
        # Find JSON block
        start = response_text.find('{')
        end = response_text.rfind('}') + 1

        if start == -1 or end == 0:
            raise ValueError("No JSON found in response")

        json_str = response_text[start:end]
        return json.loads(json_str)

    def _validate_decision(
        self,
        decision_data: Dict[str, Any],
        error_context: Dict[str, Any],
        workflow_state: Dict[str, Any]
    ) -> RecoveryDecision:
        """Validate LLM decision and apply safety checks."""

        # Validate action is in allowed set
        action = decision_data.get("action")
        valid_actions = ["needs_key", "retry_test", "skip_source", "pause_manual", "configure"]

        if action not in valid_actions:
            logger.warning(f"Invalid action '{action}', defaulting to pause_manual")
            action = "pause_manual"

        # Safety check: Don't recommend needs_key if already attempted
        if action == "needs_key" and workflow_state.get("api_key_acquisition_attempted"):
            logger.info("Overriding needs_key recommendation - already attempted acquisition")
            action = "pause_manual"
            decision_data["user_message"] = "API key acquisition already attempted. Please provide key manually."

        # Safety check: Don't retry indefinitely
        max_retries = decision_data.get("max_retries_suggestion", 3)
        if action == "retry_test" and workflow_state.get("test_attempts", 0) >= max_retries:
            logger.info(f"Overriding retry_test - already tried {workflow_state.get('test_attempts')} times")
            action = "pause_manual"
            decision_data["user_message"] = f"Maximum retry attempts ({max_retries}) exceeded."

        # Clamp confidence to valid range
        confidence = max(0.0, min(1.0, decision_data.get("confidence", 0.5)))

        return RecoveryDecision(
            action=action,
            reasoning=decision_data.get("reasoning", "No reasoning provided"),
            confidence=confidence,
            user_message=decision_data.get("user_message"),
            retry_recommended=decision_data.get("retry_recommended", False),
            max_retries_suggestion=min(5, max(1, decision_data.get("max_retries_suggestion", 3)))
        )

    def _fallback_decision(
        self,
        error_context: Dict[str, Any],
        workflow_state: Dict[str, Any]
    ) -> RecoveryDecision:
        """Fallback to rule-based decision if LLM fails."""
        logger.warning("Using fallback decision logic due to LLM failure")

        # Simple rule-based fallback
        issue = error_context.get("issue", "").lower()
        status_code = error_context.get("status_code")

        # Auth errors
        if status_code in (401, 403) or any(kw in issue for kw in ["auth", "unauthorized", "forbidden"]):
            if not workflow_state.get("api_key_acquisition_attempted"):
                return RecoveryDecision(
                    action="needs_key",
                    reasoning="Auth error detected, attempting key acquisition (fallback logic)",
                    confidence=0.7
                )

        # Network errors - retry
        if any(kw in issue for kw in ["timeout", "connection", "network"]):
            return RecoveryDecision(
                action="retry_test",
                reasoning="Network error detected, retry recommended (fallback logic)",
                confidence=0.6,
                retry_recommended=True
            )

        # Default: pause for manual input
        return RecoveryDecision(
            action="pause_manual",
            reasoning="Unable to determine recovery strategy, requesting manual input (fallback logic)",
            confidence=0.3,
            user_message="An error occurred that requires manual review."
        )

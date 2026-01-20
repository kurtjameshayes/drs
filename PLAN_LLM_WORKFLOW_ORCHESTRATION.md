# Plan: LLM-Driven Workflow Orchestration

## Executive Summary

This plan adds intelligent LLM-based decision making to workflow orchestration while maintaining rule-based safety guardrails. The goal is to make the workflow more adaptive and autonomous by replacing hard-coded keyword matching and fixed routing logic with contextual reasoning.

**Core Principle**: "LLM for judgment, rules for safety"

## Problem Statement

### Current Limitations

1. **Error Recovery is Brittle** (workflow.py:385-412)
   - Uses keyword matching: `["authentication", "401", "403", "api key"]`
   - Can't handle novel error patterns
   - Can't distinguish between temporary vs permanent failures
   - Can't reason about error context

2. **Pause Decisions are Binary** (workflow.py:178-187)
   - Any error → pause for human input
   - Can't determine if error is auto-recoverable
   - Results in unnecessary workflow interruptions
   - Poor UX when simple retry would work

3. **Fixed Routing Logic**
   - Status codes hard-coded everywhere
   - Retry counts arbitrary (`>= 2`)
   - Can't adapt strategy based on context
   - No learning from similar past failures

4. **Growing Technical Debt**
   - More edge cases → more if-elif chains
   - Hard to maintain keyword lists
   - Each new error type requires code change

### What Success Looks Like

- Workflow handles novel errors without code changes
- Fewer unnecessary pauses (better UX)
- Clear audit trail of LLM decisions
- Safety guardrails prevent runaway behavior
- Performance overhead acceptable (<500ms per decision)

---

## Architecture Overview

### Components

```
┌─────────────────────────────────────────────────────┐
│           Workflow Orchestration Layer              │
│  (workflow.py - LangGraph + Routing Logic)          │
└─────────────────┬───────────────────────────────────┘
                  │
                  ├─> Rule-Based Fast Paths (unchanged)
                  │   • Success → Continue
                  │   • Max retries exceeded → Stop
                  │
                  └─> LLM Decision Engine (NEW)
                      ├─> Error Recovery Strategy
                      ├─> Pause/Continue Determination
                      └─> Adaptive Retry Logic
                         │
                         ├─> Returns: Action + Reasoning
                         └─> Validated by Safety Checks
```

### New Module: Workflow Decision Agent

**Location**: `/home/user/drs/core/discovery/agents/workflow_decision_agent.py`

**Responsibilities**:
1. Analyze error context and recommend recovery strategy
2. Determine if workflow should pause or continue
3. Assess if retry is worthwhile
4. Provide reasoning for all decisions (audit trail)

**NOT Responsible For**:
- Enforcing max retry limits (workflow does this)
- Preventing infinite loops (workflow does this)
- Executing the recommended action (workflow does this)

---

## Detailed Implementation

### Phase 1: Error Recovery Intelligence

#### 1.1 Create WorkflowDecisionAgent

**File**: `/home/user/drs/core/discovery/agents/workflow_decision_agent.py` (NEW)

```python
"""
Intelligent decision-making agent for workflow orchestration.

Uses LLM reasoning to determine recovery strategies for errors,
pause/continue decisions, and adaptive retry logic.
"""

from typing import Dict, Any, Literal, Optional
from dataclasses import dataclass
import json
from langchain_anthropic import ChatAnthropic
from langchain_core.messages import SystemMessage, HumanMessage

from core.discovery.config import Config
from core.discovery.llm_logger import invoke_llm_with_logging
from core.utils.logging import get_logger

logger = get_logger(__name__)


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
    """

    def __init__(self):
        self.llm = ChatAnthropic(
            model=Config.DISCOVERY_LLM_MODEL,
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
```

**Key Design Decisions**:

1. **Structured Output**: `RecoveryDecision` dataclass for type safety
2. **Low Temperature**: 0.1 for consistent decisions
3. **Safety Validation**: `_validate_decision()` enforces business rules
4. **Fallback Logic**: Rule-based backup if LLM fails
5. **Logging**: All decisions logged via `invoke_llm_with_logging()`
6. **Confidence Scoring**: LLM provides confidence, workflow uses for pause decisions

---

#### 1.2 Update Workflow to Use Decision Agent

**File**: `/home/user/drs/core/discovery/workflow.py`

**Changes**:

##### A. Add Decision Agent Import and Initialization

**Location**: Top of file (around lines 1-30)

```python
# Add to imports
from core.discovery.agents.workflow_decision_agent import (
    WorkflowDecisionAgent,
    RecoveryDecision
)

# In DiscoveryWorkflow.__init__()
class DiscoveryWorkflow:
    def __init__(self, ...):
        # ... existing init code ...

        # NEW: Initialize decision agent
        self.decision_agent = WorkflowDecisionAgent()

        # Track action history for LLM context
        self.action_history = []
```

##### B. Replace _check_after_test() with Intelligent Version

**Location**: Lines 385-412

**Current Code**:
```python
def _check_after_test(self, state: DiscoveryState) -> Literal["needs_key", "configure", "stop"]:
    """Check what to do after testing."""
    # If test succeeded, go to configure
    test_results = state.get("test_results", {})
    if test_results.get("success"):
        return "configure"

    # If interrupted with auth error and haven't tried key acquisition
    if state.get("interrupted"):
        error = state.get("error", {})
        error_issue = error.get("issue", "").lower()

        # Check if it's an auth error
        if any(
            keyword in error_issue
            for keyword in ["authentication", "authorization", "401", "403", "api key"]
        ):
            # Only attempt key acquisition once
            if not state.get("api_key_acquisition_attempted"):
                logger.info("Auth error detected, attempting API key acquisition")
                return "needs_key"

    # Check if waiting for human input (may be set by testing agent)
    if state.get("waiting_for_human_input"):
        return "stop"

    # Otherwise stop (will pause for human input or end with error)
    return "stop"
```

**New Code**:
```python
def _check_after_test(self, state: DiscoveryState) -> Literal["needs_key", "configure", "stop", "retry_test"]:
    """
    Intelligently determine next action after test step.

    Uses LLM-based decision agent for complex error scenarios,
    with rule-based fast paths for simple cases.
    """
    # Fast path: Success -> configure
    test_results = state.get("test_results", {})
    if test_results.get("success"):
        logger.info("Test succeeded, proceeding to configure")
        return "configure"

    # Fast path: Explicit human input request -> stop
    if state.get("waiting_for_human_input"):
        logger.info("Workflow paused for human input")
        return "stop"

    # If test failed, use intelligent decision making
    if state.get("interrupted"):
        error = state.get("error", {})

        # Build context for decision agent
        error_context = {
            "issue": error.get("issue", "Unknown"),
            "details": error.get("details", ""),
            "status_code": test_results.get("status_code"),
            "agent_name": error.get("agent_name", "Unknown")
        }

        workflow_state = {
            "current_step": "test",
            "test_attempts": state.get("test_attempts", 0),
            "api_key_present": bool(
                state.get("access_documentation", {}).get("_provided_api_key")
            ),
            "api_key_acquisition_attempted": state.get("api_key_acquisition_attempted", False),
            "selected_source_name": state.get("selected_source", {}).get("candidate", {}).get("name", "Unknown"),
            "action_history": self.action_history
        }

        # Get LLM decision
        logger.info("Analyzing error with WorkflowDecisionAgent...")
        decision = self.decision_agent.analyze_error_recovery(
            error_context=error_context,
            workflow_state=workflow_state
        )

        # Log decision for audit trail
        logger.info(
            f"WorkflowDecisionAgent recommendation: {decision.action} "
            f"(confidence: {decision.confidence:.2f})\n"
            f"Reasoning: {decision.reasoning}"
        )

        # Store decision in state for debugging
        state["last_decision"] = {
            "action": decision.action,
            "reasoning": decision.reasoning,
            "confidence": decision.confidence,
            "timestamp": datetime.utcnow().isoformat()
        }

        # Track action in history
        self.action_history.append(
            f"test_failed -> {decision.action} ({decision.reasoning[:50]}...)"
        )

        # Safety check: Should we pause for human input?
        should_pause, pause_reason = self.decision_agent.should_pause_for_human(
            error_context=error_context,
            workflow_state=workflow_state,
            recovery_decision=decision
        )

        if should_pause:
            logger.info(f"Pausing for human input: {pause_reason}")
            state["waiting_for_human_input"] = True
            state["pause_reason"] = "error_requires_manual_review"
            state["human_input_request"] = {
                "type": "error_guidance",
                "description": decision.user_message or pause_reason,
                "context": error_context
            }
            return "stop"

        # Execute recommended action with safety checks
        if decision.action == "needs_key":
            # Safety: Only attempt acquisition once
            if state.get("api_key_acquisition_attempted"):
                logger.warning("API key acquisition already attempted, pausing instead")
                state["waiting_for_human_input"] = True
                return "stop"
            logger.info("Routing to API key acquisition")
            return "needs_key"

        elif decision.action == "retry_test":
            # Safety: Enforce max retries
            attempts = state.get("test_attempts", 0)
            max_retries = decision.max_retries_suggestion
            if attempts >= max_retries:
                logger.warning(f"Max retries ({max_retries}) exceeded, pausing")
                state["waiting_for_human_input"] = True
                state["pause_reason"] = "max_retries_exceeded"
                return "stop"

            logger.info(f"Retrying test (attempt {attempts + 1}/{max_retries})")
            state["test_attempts"] = attempts + 1
            return "retry_test"  # NEW: Need to add this routing

        elif decision.action == "configure":
            # Error is minor, proceed
            logger.info("Error deemed minor, proceeding to configure")
            state["interrupted"] = False  # Clear interrupted flag
            return "configure"

        elif decision.action == "skip_source":
            # Skip to next candidate - NOT IMPLEMENTED YET
            # For now, treat as pause
            logger.warning("skip_source not yet implemented, pausing for manual input")
            state["waiting_for_human_input"] = True
            return "stop"

        else:  # pause_manual or unknown
            logger.info("Pausing for manual input")
            state["waiting_for_human_input"] = True
            return "stop"

    # Default: stop
    return "stop"
```

**Key Changes**:
1. ✅ LLM analyzes error context
2. ✅ Returns structured decision with reasoning
3. ✅ Safety checks enforce business rules
4. ✅ Audit trail logged
5. ✅ Fallback to stop if uncertain
6. 🆕 Added "retry_test" routing option

##### C. Update LangGraph to Support retry_test

**Location**: Lines 100-176 (`_build_graph()`)

**Current**: Test step only routes to `needs_key`, `configure`, or `stop`

**Add**:
```python
def _build_graph(self) -> StateGraph:
    """Build the LangGraph workflow."""
    graph = StateGraph(DiscoveryState)

    # ... existing nodes ...

    # Add nodes
    graph.add_node("test", self._test_node)

    # Update test routing to support retry
    graph.add_conditional_edges(
        "test",
        self._check_after_test,
        {
            "needs_key": "acquire_api_key",
            "configure": "configure",
            "retry_test": "test",  # NEW: Loop back to test
            "stop": END,
        },
    )

    # ... rest of graph ...
```

**Safety Check**: Add retry counter to prevent infinite loops (already in plan above with `test_attempts`)

---

#### 1.3 Add Prompts to prompts.py

**File**: `/home/user/drs/core/discovery/prompts.py`

**Location**: End of file (after existing agent prompts)

```python
# Workflow Decision Agent Prompts
WORKFLOW_DECISION_SYSTEM = """You are a workflow orchestration decision agent for an API discovery system.

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

WORKFLOW_DECISION_TASK_TEMPLATE = """Analyze this error and recommend a recovery strategy:

## Error Context
- Issue: {issue}
- Details: {details}
- HTTP Status Code: {status_code}
- Agent: {agent_name}

## Workflow State
- Current Step: {current_step}
- Test Attempts So Far: {test_attempts}
- API Key Present: {api_key_present}
- API Key Acquisition Attempted: {api_key_acquisition_attempted}
- Selected Source: {selected_source_name}

## Previous Actions
{action_history}

Based on this context, what is the best recovery strategy?
"""
```

---

### Phase 2: Enhanced Logging & Audit Trail

#### 2.1 Add Decision Logging to LLM Logger

**File**: `/home/user/drs/core/discovery/llm_logger.py`

**Enhancement**: Add structured decision logging

```python
def log_workflow_decision(
    decision: RecoveryDecision,
    error_context: Dict[str, Any],
    workflow_state: Dict[str, Any],
    execution_time_ms: float
) -> None:
    """
    Log workflow decision for audit trail and debugging.

    Creates a structured log entry with all context needed to
    understand why a particular routing decision was made.
    """
    log_entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "decision_type": "workflow_routing",
        "action": decision.action,
        "reasoning": decision.reasoning,
        "confidence": decision.confidence,
        "execution_time_ms": execution_time_ms,
        "error_context": error_context,
        "workflow_state": {
            "current_step": workflow_state.get("current_step"),
            "test_attempts": workflow_state.get("test_attempts"),
            "api_key_present": workflow_state.get("api_key_present"),
        },
    }

    logger.info(f"Workflow Decision: {json.dumps(log_entry, indent=2)}")

    # TODO: Optionally persist to database for analytics
```

#### 2.2 Store Decisions in Workflow State

**Purpose**: Enable debugging and decision replay

**In workflow.py**: Store decision history in state

```python
# After getting decision from agent
state.setdefault("decision_history", []).append({
    "timestamp": datetime.utcnow().isoformat(),
    "step": "test",
    "decision": decision.action,
    "reasoning": decision.reasoning,
    "confidence": decision.confidence,
})
```

---

### Phase 3: Testing & Validation

#### 3.1 Unit Tests for WorkflowDecisionAgent

**File**: `/home/user/drs/tests/test_workflow_decision_agent.py` (NEW)

```python
"""Unit tests for WorkflowDecisionAgent."""

import pytest
from unittest.mock import Mock, patch
from core.discovery.agents.workflow_decision_agent import (
    WorkflowDecisionAgent,
    RecoveryDecision
)


class TestWorkflowDecisionAgent:
    """Test suite for WorkflowDecisionAgent."""

    def setup_method(self):
        """Setup test fixtures."""
        self.agent = WorkflowDecisionAgent()

    def test_auth_error_recommends_needs_key(self):
        """Test that 401 errors recommend needs_key action."""
        error_context = {
            "issue": "Authentication required",
            "details": "API key missing",
            "status_code": 401,
            "agent_name": "TestingAgent"
        }
        workflow_state = {
            "current_step": "test",
            "test_attempts": 1,
            "api_key_present": False,
            "api_key_acquisition_attempted": False,
        }

        with patch.object(self.agent, 'llm') as mock_llm:
            mock_llm.invoke.return_value = Mock(content='''
            {
                "action": "needs_key",
                "reasoning": "401 status indicates missing authentication",
                "confidence": 0.95,
                "retry_recommended": false,
                "max_retries_suggestion": 3
            }
            ''')

            decision = self.agent.analyze_error_recovery(error_context, workflow_state)

            assert decision.action == "needs_key"
            assert decision.confidence > 0.9
            assert "authentication" in decision.reasoning.lower()

    def test_network_error_recommends_retry(self):
        """Test that network errors recommend retry action."""
        error_context = {
            "issue": "Connection timeout",
            "details": "Failed to connect to server",
            "status_code": None,
            "agent_name": "TestingAgent"
        }
        workflow_state = {
            "current_step": "test",
            "test_attempts": 1,
            "api_key_present": True,
        }

        with patch.object(self.agent, 'llm') as mock_llm:
            mock_llm.invoke.return_value = Mock(content='''
            {
                "action": "retry_test",
                "reasoning": "Timeout is likely temporary, retry recommended",
                "confidence": 0.8,
                "retry_recommended": true,
                "max_retries_suggestion": 3
            }
            ''')

            decision = self.agent.analyze_error_recovery(error_context, workflow_state)

            assert decision.action == "retry_test"
            assert decision.retry_recommended is True

    def test_safety_check_prevents_infinite_key_acquisition(self):
        """Test that safety checks prevent retrying key acquisition."""
        error_context = {
            "issue": "Authentication failed",
            "status_code": 401,
        }
        workflow_state = {
            "api_key_acquisition_attempted": True,  # Already tried
        }

        with patch.object(self.agent, 'llm') as mock_llm:
            # LLM recommends needs_key, but should be overridden
            mock_llm.invoke.return_value = Mock(content='''
            {
                "action": "needs_key",
                "reasoning": "Auth error",
                "confidence": 0.9,
                "retry_recommended": false,
                "max_retries_suggestion": 3
            }
            ''')

            decision = self.agent.analyze_error_recovery(error_context, workflow_state)

            # Should be overridden to pause_manual
            assert decision.action == "pause_manual"

    def test_safety_check_prevents_excessive_retries(self):
        """Test that safety checks prevent infinite retry loops."""
        error_context = {
            "issue": "Connection timeout",
        }
        workflow_state = {
            "test_attempts": 5,  # Already tried 5 times
        }

        with patch.object(self.agent, 'llm') as mock_llm:
            # LLM recommends retry with max 3 attempts
            mock_llm.invoke.return_value = Mock(content='''
            {
                "action": "retry_test",
                "reasoning": "Temporary error",
                "confidence": 0.7,
                "retry_recommended": true,
                "max_retries_suggestion": 3
            }
            ''')

            decision = self.agent.analyze_error_recovery(error_context, workflow_state)

            # Should be overridden to pause_manual
            assert decision.action == "pause_manual"

    def test_fallback_on_llm_failure(self):
        """Test graceful fallback when LLM fails."""
        error_context = {
            "issue": "Authentication required",
            "status_code": 401,
        }
        workflow_state = {
            "api_key_acquisition_attempted": False,
        }

        with patch.object(self.agent, 'llm') as mock_llm:
            mock_llm.invoke.side_effect = Exception("LLM service unavailable")

            decision = self.agent.analyze_error_recovery(error_context, workflow_state)

            # Should use fallback logic
            assert decision.action == "needs_key"  # Fallback recognizes 401
            assert decision.confidence < 1.0  # Lower confidence for fallback

    def test_confidence_threshold_triggers_pause(self):
        """Test that low confidence decisions trigger pause."""
        error_context = {"issue": "Unknown error"}
        workflow_state = {"test_attempts": 1}

        decision = RecoveryDecision(
            action="retry_test",
            reasoning="Uncertain",
            confidence=0.3  # Low confidence
        )

        should_pause, reason = self.agent.should_pause_for_human(
            error_context, workflow_state, decision
        )

        assert should_pause is True
        assert "low confidence" in reason.lower()
```

#### 3.2 Integration Tests

**File**: `/home/user/drs/tests/integration/test_workflow_intelligent_routing.py` (NEW)

```python
"""Integration tests for LLM-driven workflow routing."""

import pytest
from core.discovery.workflow import DiscoveryWorkflow
from core.discovery.state import DiscoveryState


@pytest.mark.integration
class TestIntelligentWorkflowRouting:
    """Test end-to-end workflow with intelligent routing."""

    def test_auth_error_routes_to_key_acquisition(self, mock_workflow_dependencies):
        """Test that auth errors route to API key acquisition."""
        workflow = DiscoveryWorkflow(...)

        # Simulate test failure with 401
        state = {
            "interrupted": True,
            "error": {
                "issue": "Authentication required",
                "details": "HTTP 401",
            },
            "test_results": {
                "success": False,
                "status_code": 401,
            },
            "test_attempts": 1,
        }

        next_step = workflow._check_after_test(state)

        assert next_step == "needs_key"
        assert "last_decision" in state
        assert state["last_decision"]["action"] == "needs_key"

    def test_network_timeout_retries_then_pauses(self, mock_workflow_dependencies):
        """Test that network timeouts retry, then pause after max attempts."""
        workflow = DiscoveryWorkflow(...)

        # First attempt - should retry
        state = {
            "interrupted": True,
            "error": {"issue": "Connection timeout"},
            "test_results": {"success": False},
            "test_attempts": 1,
        }

        next_step = workflow._check_after_test(state)
        assert next_step == "retry_test"

        # Third attempt - should pause
        state["test_attempts"] = 3
        next_step = workflow._check_after_test(state)
        assert next_step == "stop"
        assert state.get("waiting_for_human_input") is True
```

#### 3.3 Test Scenarios

Create test suite covering:

| Scenario | Expected Action | Validation |
|----------|----------------|------------|
| 401 error, no key | needs_key | Routes to acquire_api_key |
| 403 error, key present | retry_test or pause | Depends on context |
| Network timeout, attempt 1 | retry_test | Increments test_attempts |
| Network timeout, attempt 3 | pause_manual | Sets waiting_for_human_input |
| Rate limit (429) | retry_test | Suggests backoff |
| Invalid URL (404) | skip_source | Moves to next candidate |
| Data format error | configure | Proceeds despite error |
| Unknown error | pause_manual | Conservative fallback |

---

### Phase 4: Monitoring & Metrics

#### 4.1 Decision Metrics

Track in database or logs:

```python
# Metrics to collect
{
    "decision_latency_ms": float,
    "action_taken": str,
    "confidence_score": float,
    "safety_override": bool,  # Was LLM decision overridden?
    "llm_failure": bool,      # Did we use fallback?
    "outcome": str,           # "success" | "pause" | "error"
}
```

#### 4.2 Dashboard / Logging Queries

Questions to answer:
- What % of errors are auto-recovered vs pause?
- What's the average confidence score for each action type?
- How often are safety checks overriding LLM decisions?
- What's the latency impact of LLM decisions?
- Are there error patterns the LLM handles poorly?

---

## Migration Strategy

### Step 1: Feature Flag (Gradual Rollout)

```python
# In config.py
class Config:
    # Feature flag for LLM-driven routing
    ENABLE_INTELLIGENT_WORKFLOW_ROUTING = os.getenv(
        "ENABLE_INTELLIGENT_WORKFLOW_ROUTING",
        "false"
    ).lower() == "true"
```

```python
# In workflow.py _check_after_test()
def _check_after_test(self, state: DiscoveryState):
    if Config.ENABLE_INTELLIGENT_WORKFLOW_ROUTING:
        return self._check_after_test_intelligent(state)
    else:
        return self._check_after_test_legacy(state)
```

### Step 2: A/B Testing

Run both approaches in parallel:
- 50% of workflows use new LLM routing
- 50% use legacy rule-based routing
- Compare metrics:
  - Success rate
  - Pause frequency
  - User satisfaction
  - Latency

### Step 3: Gradual Rollout

1. **Week 1**: 10% traffic on LLM routing
2. **Week 2**: 25% traffic (if metrics good)
3. **Week 3**: 50% traffic
4. **Week 4**: 100% traffic
5. **Week 5**: Remove legacy code

### Step 4: Deprecate Legacy Code

Once LLM routing is validated:
1. Remove `_check_after_test_legacy()`
2. Remove feature flag
3. Update documentation
4. Archive old test cases

---

## Performance Considerations

### Latency Impact

**LLM Decision Call**:
- Expected: 200-500ms per decision
- Acceptable: <1 second
- Monitoring: Log all decision latencies

**Mitigation**:
- Use fast paths for simple cases (success, explicit pause)
- Cache common error patterns
- Use haiku model if latency is critical

### Cost Impact

**Per Workflow**:
- 0-3 LLM decision calls (only on errors)
- ~500 tokens per call (prompt + response)
- Cost: ~$0.01-0.03 per workflow with errors

**Annual** (assuming 10,000 workflows, 30% encounter errors):
- 3,000 workflows * 2 decisions * $0.02 = $120/year
- Negligible compared to developer time savings

### Caching Strategy

```python
# Cache common error patterns
ERROR_CACHE = {
    "hash(401 + no_key)": RecoveryDecision(action="needs_key", ...),
    "hash(timeout + attempt_1)": RecoveryDecision(action="retry_test", ...),
}

def analyze_error_recovery(self, error_context, workflow_state):
    cache_key = self._compute_cache_key(error_context, workflow_state)
    if cache_key in ERROR_CACHE:
        return ERROR_CACHE[cache_key]
    # ... proceed with LLM call
```

---

## Risks & Mitigations

### Risk 1: LLM Makes Bad Decisions

**Likelihood**: Medium
**Impact**: High (workflow fails or pauses unnecessarily)

**Mitigations**:
1. ✅ Safety validation overrides unsafe LLM decisions
2. ✅ Fallback to conservative rule-based logic on LLM failure
3. ✅ Feature flag for easy rollback
4. ✅ Extensive testing before rollout
5. ✅ Monitor decision quality metrics

### Risk 2: Increased Latency

**Likelihood**: High
**Impact**: Medium (users wait longer)

**Mitigations**:
1. ✅ Fast paths for simple cases (no LLM call)
2. ✅ Use haiku model for speed
3. ✅ Cache common patterns
4. ✅ Async LLM calls where possible
5. ✅ Set timeout on LLM calls (5s max)

### Risk 3: Increased Cost

**Likelihood**: High
**Impact**: Low ($120/year is negligible)

**Mitigations**:
1. ✅ Only call LLM on errors (not every step)
2. ✅ Cache results for similar errors
3. ✅ Use smaller model (haiku) if cost becomes issue
4. ✅ Monitor token usage

### Risk 4: Unpredictable Behavior

**Likelihood**: Medium
**Impact**: Medium (harder to debug)

**Mitigations**:
1. ✅ Comprehensive logging of all decisions
2. ✅ Store reasoning in state for debugging
3. ✅ Low temperature (0.1) for consistency
4. ✅ Structured JSON output (less variability)
5. ✅ Decision replay capability for debugging

### Risk 5: LLM Service Downtime

**Likelihood**: Low
**Impact**: High (workflows fail)

**Mitigations**:
1. ✅ Fallback to rule-based logic on LLM failure
2. ✅ Retry with exponential backoff
3. ✅ Circuit breaker pattern (stop calling LLM if failing)
4. ✅ Cache recent decisions for reuse

---

## Success Criteria

### Must Have
- [ ] WorkflowDecisionAgent implemented with all safety checks
- [ ] Workflow routing uses LLM for error recovery
- [ ] Fallback logic works when LLM fails
- [ ] All unit tests pass
- [ ] Integration tests cover key scenarios
- [ ] Feature flag allows safe rollout
- [ ] Decision logging provides audit trail

### Should Have
- [ ] Latency <500ms per decision
- [ ] 80%+ of errors auto-recovered (no pause)
- [ ] Safety overrides < 5% of decisions
- [ ] Comprehensive test coverage (>90%)

### Nice to Have
- [ ] Decision caching for common patterns
- [ ] Metrics dashboard
- [ ] A/B testing infrastructure
- [ ] Decision replay for debugging

---

## Implementation Timeline

### Week 1: Core Implementation
- Day 1-2: Implement WorkflowDecisionAgent
- Day 3-4: Update workflow.py to use decision agent
- Day 5: Add feature flag and logging

### Week 2: Testing & Safety
- Day 1-2: Write unit tests
- Day 3: Write integration tests
- Day 4: Manual testing of edge cases
- Day 5: Safety check validation

### Week 3: Rollout & Monitoring
- Day 1: Deploy with feature flag disabled
- Day 2: Enable for 10% of traffic
- Day 3-4: Monitor metrics, fix issues
- Day 5: Increase to 25%

### Week 4: Full Deployment
- Day 1: Increase to 50%
- Day 2-3: Monitor and optimize
- Day 4: Increase to 100%
- Day 5: Documentation and cleanup

---

## Open Questions

1. **Should we support decision caching?**
   - Pro: Faster, more consistent
   - Con: Might miss context-specific nuances
   - **Decision**: Start without cache, add if latency is issue

2. **What model should we use?**
   - Options: sonnet-4-5 (current), haiku-4 (faster/cheaper)
   - **Decision**: Start with sonnet-4-5, switch to haiku if needed

3. **How do we handle decision disagreement (LLM vs safety checks)?**
   - **Decision**: Safety checks always win, log disagreements

4. **Should we expose decision reasoning to users?**
   - **Decision**: Yes, in pause messages show why we're pausing

5. **Do we need a separate model for workflow decisions vs agent tasks?**
   - **Decision**: Use same model, but low temp (0.1) for consistency

---

## Related Plans

This plan builds on and complements:
- **PLAN.md**: API key workflow improvements (acquire_api_key before manual input)
- Both plans can be implemented independently but work together

---

## Appendix: Example Prompts & Responses

### Example 1: Auth Error

**Input**:
```
Error: Authentication required (HTTP 401)
Status: 401
Attempts: 1
API Key Present: False
```

**Expected Response**:
```json
{
  "action": "needs_key",
  "reasoning": "HTTP 401 indicates missing authentication. API key is not present and acquisition has not been attempted. Route to API key acquisition for automatic retrieval via email or manual input.",
  "confidence": 0.95,
  "user_message": null,
  "retry_recommended": false,
  "max_retries_suggestion": 3
}
```

### Example 2: Network Timeout

**Input**:
```
Error: Connection timeout
Details: Failed to connect to api.example.com after 30s
Attempts: 1
```

**Expected Response**:
```json
{
  "action": "retry_test",
  "reasoning": "Connection timeout is likely a temporary network issue. One retry attempt is reasonable before considering the source unreachable.",
  "confidence": 0.85,
  "user_message": null,
  "retry_recommended": true,
  "max_retries_suggestion": 3
}
```

### Example 3: Unknown Error

**Input**:
```
Error: Unexpected response format
Details: Expected JSON, received HTML
Attempts: 2
```

**Expected Response**:
```json
{
  "action": "pause_manual",
  "reasoning": "Unexpected response format after 2 attempts suggests the API endpoint or documentation may be incorrect. Manual review needed to verify the correct endpoint and expected format.",
  "confidence": 0.70,
  "user_message": "The API returned HTML instead of JSON. Please verify the endpoint URL is correct and supports the expected data format.",
  "retry_recommended": false,
  "max_retries_suggestion": 3
}
```

---

## Summary

This plan introduces intelligent LLM-based decision making to workflow orchestration while maintaining safety through rule-based guardrails. The implementation is incremental, testable, and reversible via feature flags.

**Key Benefits**:
1. Handles novel errors without code changes
2. Reduces unnecessary pauses
3. Clear audit trail of decisions
4. Safe rollout with fallbacks
5. Low cost and latency impact

**Next Steps**:
1. Review and approve plan
2. Implement WorkflowDecisionAgent
3. Add tests
4. Deploy with feature flag
5. Monitor and optimize

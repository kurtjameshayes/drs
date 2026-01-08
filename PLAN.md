# Plan: Fix API Key Workflow to Prefer Automatic Acquisition

## Problem Statement

Currently, when the TestingAgent detects an authentication error (401/403), it **immediately** sets `waiting_for_human_input = True` and creates a `HumanInputRequest`. This is premature because:

1. The workflow already has an `acquire_api_key` step with the APIKeyAgent designed to **automatically** acquire API keys
2. The APIKeyAgent can:
   - Check for existing stored keys
   - Poll email for API keys (if Gmail configured)
   - Analyze registration pages
   - Guide users through registration with email verification
3. Only after exhausting all automatic methods should the system ask for manual input

**The issue:** By setting `waiting_for_human_input = True` in the TestingAgent, we're signaling "need human help NOW" before giving the automated acquisition system a chance to work.

## Current Flow

```
Test fails (401/403)
  ↓
TestingAgent detects auth error
  ↓
TestingAgent sets:
  - interrupted = True ✓
  - waiting_for_human_input = True ✗ (PREMATURE!)
  - human_input_request = {...} ✗ (PREMATURE!)
  ↓
Workflow._check_after_test():
  - Sees interrupted + auth error + not api_key_acquisition_attempted
  - Returns "needs_key" → routes to acquire_api_key ✓
  ↓
acquire_api_key_node runs APIKeyAgent:
  - Checks existing keys
  - Tries email acquisition
  - Falls back to _request_manual_intervention()
  - Sets waiting_for_human_input = True (NOW is appropriate)
  ↓
Workflow._check_after_key_acquisition():
  - If key acquired: returns "retry_test"
  - If not: returns "stop" (workflow pauses for manual input)
```

**The problem:** The TestingAgent is setting human input flags that logically belong to the APIKeyAgent.

## Proposed Solution

### Principle: Separation of Concerns

1. **TestingAgent**: Detect and report auth errors, but don't request human input
2. **Workflow**: Route auth errors to the acquire_api_key step
3. **APIKeyAgent**: Be the ONLY agent that requests human input for API keys (after exhausting automatic methods)

### Changes Required

#### 1. TestingAgent - Remove Premature Human Input Request

**File**: `/home/user/drs/core/discovery/agents/testing_agent.py`

**Location**: Lines 433-458 (auth error handling, first time - no API key provided)

**Current Code**:
```python
# No API key was provided yet - first time asking
state["error"] = WorkflowError(
    agent_name="TestingAgent",
    step="testing",
    issue=f"Authentication required (HTTP {status_code})",
    details=f"The API requires authentication. Type: {auth_type}...",
    recoverable=True,
).to_dict()
state["interrupted"] = True

# ❌ REMOVE THESE - Let APIKeyAgent handle it
state["waiting_for_human_input"] = True
state["pause_reason"] = "needs_api_key"
state["human_input_request"] = HumanInputRequest(...).to_dict()
```

**New Code**:
```python
# No API key was provided yet - signal that auth is needed
state["error"] = WorkflowError(
    agent_name="TestingAgent",
    step="testing",
    issue=f"Authentication required (HTTP {status_code})",
    details=f"The API requires authentication. Type: {auth_type}...",
    recoverable=True,
).to_dict()
state["interrupted"] = True

# Store auth details for the APIKeyAgent to use
state["auth_requirement"] = {
    "auth_type": auth_type,
    "auth_header": auth.get("auth_header"),
    "registration_url": registration_url,
    "source_name": source_name,
    "detected_by": "testing_agent",
}

# ✅ DO NOT set waiting_for_human_input here
# Let the workflow route to acquire_api_key
# Let the APIKeyAgent try automatic acquisition first
# APIKeyAgent will set waiting_for_human_input if needed
```

**Why**:
- TestingAgent's job is to TEST, not to manage the acquisition workflow
- By storing `auth_requirement` details, we pass context to the APIKeyAgent
- The workflow will see `interrupted = True` + auth error and route to `acquire_api_key`
- APIKeyAgent gets all the info it needs from `auth_requirement`

#### 2. APIKeyAgent - Use Auth Requirement Details

**File**: `/home/user/drs/core/discovery/agents/api_key_agent.py`

**Location**: Lines 58-147 (main `run()` method)

**Current**: The APIKeyAgent extracts auth info from `access_documentation`

**Enhancement**: Also check for `state.get("auth_requirement", {})` for TestingAgent-provided details

**Rationale**:
- TestingAgent has already detected and extracted auth details
- APIKeyAgent should reuse this information instead of re-extracting
- Improves consistency and reduces redundant work

**Specific Changes**:

In `run()` method around lines 77-116:
```python
# Get auth and source information
access_documentation = state.get("access_documentation", {})

# ✅ NEW: Check if TestingAgent already extracted auth details
auth_requirement = state.get("auth_requirement", {})

# Prefer TestingAgent-extracted details if available
auth = auth_requirement if auth_requirement else access_documentation.get("authentication", {})
source_name = auth_requirement.get("source_name") or access_documentation.get("source_name", "Unknown")
base_url = access_documentation.get("base_url", "")
registration_url = auth_requirement.get("registration_url") or auth.get("registration_url")
auth_type = auth_requirement.get("auth_type") or auth.get("auth_type", "api_key")
```

**Why**:
- Reuses the auth details TestingAgent already discovered
- Avoids duplicate extraction logic
- More reliable since TestingAgent discovered this from actual test failure

#### 3. State Schema - Add auth_requirement Field

**File**: `/home/user/drs/core/discovery/state.py`

**Location**: Around line 277+ (in DiscoveryState TypedDict or wherever state fields are documented)

**Addition**:
```python
# Optional field in DiscoveryState
auth_requirement: Optional[Dict[str, Any]]  # Auth details detected by testing
```

**Contents**:
```python
{
    "auth_type": str,           # "api_key", "oauth", etc.
    "auth_header": str,         # "Authorization", "X-API-Key", etc.
    "registration_url": str,    # URL to register for API key
    "source_name": str,         # Name of the data source
    "detected_by": str,         # Which agent detected this (e.g., "testing_agent")
}
```

**Why**:
- Formalizes the contract between TestingAgent and APIKeyAgent
- Documents the expected structure
- Makes it clear this is metadata about auth requirements

#### 4. Workflow - Ensure Proper Routing (Verification)

**File**: `/home/user/drs/core/discovery/workflow.py`

**Location**: Lines 385-412 (`_check_after_test()`)

**Current Logic** (KEEP AS IS - it's correct):
```python
def _check_after_test(self, state: DiscoveryState) -> Literal["needs_key", "configure", "stop"]:
    # ... success check ...

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
                return "needs_key"  # ✅ Routes to acquire_api_key

    # Check if waiting for human input
    if state.get("waiting_for_human_input"):
        return "stop"  # ✅ Only stops if APIKeyAgent set this

    return "stop"
```

**Verification**: This logic is CORRECT and doesn't need changes:
- ✓ Routes to `acquire_api_key` when auth error detected
- ✓ Only stops if `waiting_for_human_input = True` (which APIKeyAgent sets)
- ✓ Prevents infinite loops with `api_key_acquisition_attempted` flag

#### 5. Testing Agent - Handle Retry After Key Acquisition

**File**: `/home/user/drs/core/discovery/agents/testing_agent.py`

**Location**: Lines 375-431 (handling provided API key that failed)

**Current Behavior**:
- If a provided API key fails (401/403 after key was applied)
- Increments retry count
- After 2 attempts, asks for new key

**Verification**: This logic should REMAIN as-is because:
- This is a DIFFERENT scenario: the user/system provided a key, and it failed
- This is appropriate to ask for a new key (the old one is invalid)
- The retry limit prevents infinite loops

**No changes needed** in this section.

#### 6. Documentation Updates

**File**: `/home/user/drs/core/discovery/README.md` (or similar docs)

**Add section**:

```markdown
## Authentication & API Key Workflow

When a data source requires authentication:

1. **TestingAgent** detects auth errors (401/403)
   - Sets `interrupted = True` with error details
   - Stores auth requirements in `state["auth_requirement"]`
   - Does NOT request human input yet

2. **Workflow** routes to `acquire_api_key` step
   - Happens automatically when auth error detected
   - Attempts automatic acquisition before asking user

3. **APIKeyAgent** attempts automatic acquisition
   - Checks existing stored keys
   - Polls email for API keys (if configured)
   - Analyzes registration pages
   - Only requests manual input as last resort

4. **Manual Input** (only if automatic methods fail)
   - APIKeyAgent sets `waiting_for_human_input = True`
   - Workflow pauses for user to provide key
   - User resumes with API key via `/resume` endpoint

This ensures maximum automation before requiring user intervention.
```

## Benefits of This Approach

### 1. **Better User Experience**
- Users aren't immediately asked for API keys when automatic acquisition might work
- Email polling can retrieve keys without user intervention
- Only asks for help when truly needed

### 2. **Clearer Separation of Concerns**
- TestingAgent: Detects and reports auth problems
- Workflow: Routes based on problem type
- APIKeyAgent: Manages acquisition strategy (auto → manual)

### 3. **Reduced Interruptions**
- Fewer workflow pauses
- More autonomous operation
- Manual input only as last resort

### 4. **Better Error Messages**
- When APIKeyAgent requests input, it can say:
  - "Tried email polling: no key found"
  - "Tried existing storage: no valid key"
  - "Registration requires manual completion"
- More context about why manual input is needed

### 5. **Consistency**
- APIKeyAgent is the single source of truth for "do we need human help?"
- No conflicting signals between agents

## Testing Strategy

After implementation, test these scenarios:

### Test 1: Automatic Acquisition Success
1. Start discovery on API requiring key
2. TestingAgent detects 401/403
3. Workflow routes to acquire_api_key
4. APIKeyAgent finds key in storage OR email
5. Test retries with key and succeeds
6. **Expected**: No user prompt, workflow completes

### Test 2: Manual Input Required
1. Start discovery on API requiring key
2. TestingAgent detects 401/403
3. Workflow routes to acquire_api_key
4. APIKeyAgent exhausts automatic methods
5. APIKeyAgent sets waiting_for_human_input
6. **Expected**: Workflow pauses, requests manual key

### Test 3: Invalid Key Retry
1. User provides invalid API key
2. TestingAgent tests with key, gets 401/403
3. TestingAgent increments retry count
4. After 2 attempts, requests new key
5. **Expected**: Asks for new key (this path unchanged)

### Test 4: Email Polling Success
1. API requires key with email registration
2. User has Gmail configured
3. User registers for API key
4. APIKeyAgent polls email and finds key
5. **Expected**: Key acquired automatically, no manual input

## Implementation Order

1. **First**: Update TestingAgent (remove premature human input request)
2. **Second**: Update APIKeyAgent (use auth_requirement if available)
3. **Third**: Add auth_requirement to state schema documentation
4. **Fourth**: Test all scenarios
5. **Fifth**: Update documentation

## Risks & Mitigations

### Risk 1: Breaking Existing Workflows
**Mitigation**:
- The workflow routing logic is unchanged
- APIKeyAgent still sets waiting_for_human_input when needed
- Just moving WHERE that flag gets set

### Risk 2: Missing Auth Details
**Mitigation**:
- APIKeyAgent should fallback to extracting from access_documentation
- Use `auth_requirement.get(key) or access_documentation.get(key)`
- Graceful degradation if TestingAgent doesn't set auth_requirement

### Risk 3: Infinite Loops
**Mitigation**:
- `api_key_acquisition_attempted` flag prevents re-trying acquisition
- Retry counters in TestingAgent prevent infinite key testing
- Existing safeguards remain in place

## Success Criteria

- [ ] TestingAgent does NOT set `waiting_for_human_input` on first auth error
- [ ] Workflow routes to `acquire_api_key` on auth errors
- [ ] APIKeyAgent tries automatic acquisition before requesting input
- [ ] APIKeyAgent is the ONLY place that sets `waiting_for_human_input` for API keys
- [ ] All existing tests pass
- [ ] New test cases for automatic acquisition pass
- [ ] Documentation updated to reflect new flow

## Open Questions

1. Should we preserve the `human_input_request` structure from TestingAgent for debugging?
   - **Answer**: No, store in `auth_requirement` instead for clarity

2. What if APIKeyAgent needs additional info from TestingAgent?
   - **Answer**: Use the `auth_requirement` dict to pass any needed details

3. Should we support other auth types (OAuth, basic auth) with this pattern?
   - **Answer**: Yes, the same principle applies - let specialized agents handle acquisition

---

## Summary

**Core Change**: TestingAgent detects auth errors but doesn't request human input. It trusts the workflow to route to APIKeyAgent, which tries automatic acquisition before asking for help.

**Files to Modify**:
1. `/home/user/drs/core/discovery/agents/testing_agent.py` - Remove premature human input request
2. `/home/user/drs/core/discovery/agents/api_key_agent.py` - Use auth_requirement details
3. `/home/user/drs/core/discovery/state.py` - Document auth_requirement field
4. Documentation files - Explain new workflow

**Result**: More autonomous, less user interruption, clearer responsibilities.

# API Key Gathering Agent - Implementation Plan

## Overview

Create a new agent system to automatically gather API keys for data sources during the discovery workflow. When the Testing Agent detects that an API key is required, instead of pausing for human input, the workflow will invoke the new API Key Agent to automatically:

1. Check if an API key already exists for the data source
2. Navigate to the data source's website to find registration instructions
3. Request an API key using the configured email (DISCOVERY_EMAIL)
4. Monitor email (via Arcade API) for the API key
5. Return the key to continue the workflow

## Architecture

### New Components

```
core/discovery/
├── agents/
│   ├── api_key_agent.py           # NEW: Main orchestrating agent
│   └── ...existing agents...
├── services/
│   ├── __init__.py                # NEW
│   ├── arcade_email_service.py    # NEW: Arcade API Gmail integration
│   ├── api_key_store.py           # NEW: API key storage/retrieval
│   └── web_navigator.py           # NEW: Selenium web navigation
├── state.py                       # MODIFY: Add API key acquisition state
└── workflow.py                    # MODIFY: Integrate API key agent
```

### New Environment Variables

Add to `config.py`:

```python
# API Key Gathering Configuration
ARCADE_API_KEY = os.getenv("ARCADE_API_KEY", "")           # For Arcade API access
DISCOVERY_EMAIL = os.getenv("DISCOVERY_EMAIL", "")          # Email for API key registration
ARCADE_USER_ID = os.getenv("ARCADE_USER_ID", "")           # Arcade user identifier
API_KEY_CHECK_INTERVAL = int(os.getenv("API_KEY_CHECK_INTERVAL", 30))  # Seconds between email checks
API_KEY_CHECK_MAX_ATTEMPTS = int(os.getenv("API_KEY_CHECK_MAX_ATTEMPTS", 20))  # Max email check attempts
```

---

## Component Details

### 1. API Key Store Service (`services/api_key_store.py`)

Manages storage and retrieval of API keys in MongoDB.

```python
class APIKeyStore:
    """
    Stores and retrieves API keys for data sources.
    Keys are stored encrypted in MongoDB.
    """

    def __init__(self, db_client: MongoClient):
        self.collection = db_client[Config.DATABASE_NAME]["api_keys"]

    def get_key(self, source_identifier: str) -> Optional[str]:
        """
        Retrieve API key for a data source.

        Args:
            source_identifier: Data source URL or normalized name

        Returns:
            Decrypted API key or None if not found
        """
        pass

    def store_key(self, source_identifier: str, api_key: str, metadata: Dict) -> bool:
        """
        Store API key for a data source.

        Args:
            source_identifier: Data source URL or normalized name
            api_key: The API key to store
            metadata: Additional info (registration date, source name, etc.)
        """
        pass

    def key_exists(self, source_identifier: str) -> bool:
        """Check if we have a key for this source."""
        pass

    def invalidate_key(self, source_identifier: str) -> bool:
        """Mark a key as invalid (e.g., after auth failure)."""
        pass
```

**MongoDB Collection Schema: `api_keys`**
```json
{
    "_id": ObjectId,
    "source_identifier": "string",      // Normalized URL or source name
    "source_name": "string",            // Human-readable name
    "api_key_encrypted": "string",      // Encrypted API key
    "registration_email": "string",     // Email used to register
    "registration_url": "string",       // Where we registered
    "acquired_at": "datetime",
    "last_validated": "datetime",
    "status": "active|invalid|expired",
    "metadata": {}
}
```

---

### 2. Arcade Email Service (`services/arcade_email_service.py`)

Wraps Arcade API for Gmail operations.

```python
from arcadepy import Arcade, AsyncArcade

class ArcadeEmailService:
    """
    Service for checking email via Arcade API.
    Used to retrieve API keys sent via email after registration.
    """

    def __init__(self):
        self.client = Arcade(api_key=Config.ARCADE_API_KEY)
        self.user_id = Config.ARCADE_USER_ID or Config.DISCOVERY_EMAIL

    async def authorize_gmail(self) -> Dict[str, Any]:
        """
        Initialize Gmail authorization via Arcade.
        Returns authorization URL if user consent needed.
        """
        result = self.client.tools.authorize(
            tool_name="Gmail.ListEmails",
            user_id=self.user_id
        )
        return result

    async def search_for_api_key(
        self,
        sender_domain: str,
        subject_keywords: List[str],
        since_minutes: int = 30
    ) -> Optional[Dict[str, Any]]:
        """
        Search for API key email from a data source.

        Args:
            sender_domain: Email domain of the data source (e.g., "usda.gov")
            subject_keywords: Keywords to search for in subject
            since_minutes: Only check emails from last N minutes

        Returns:
            Email content if found, None otherwise
        """
        # Build Gmail search query
        query_parts = [f"from:*@{sender_domain}"]
        if subject_keywords:
            query_parts.append(f"subject:({' OR '.join(subject_keywords)})")
        query_parts.append(f"newer_than:{since_minutes}m")

        query = " ".join(query_parts)

        result = await self.client.tools.execute(
            tool_name="Gmail.SearchEmails",
            input={"query": query, "max_results": 5},
            user_id=self.user_id
        )

        return result

    async def get_email_content(self, email_id: str) -> Dict[str, Any]:
        """Get full email content by ID."""
        pass

    async def extract_api_key_from_email(self, email_content: str) -> Optional[str]:
        """
        Use LLM to extract API key from email content.

        Looks for patterns like:
        - "Your API key is: ..."
        - "API Key: ..."
        - Long alphanumeric strings
        """
        pass
```

---

### 3. Web Navigator Service (`services/web_navigator.py`)

Handles web navigation for API key registration using Selenium.

```python
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait

class WebNavigator:
    """
    Selenium-based web navigator for API key registration.
    """

    def __init__(self):
        self.driver = None
        self.llm = ChatAnthropic(model=Config.DISCOVERY_LLM_MODEL)

    def _init_driver(self):
        """Initialize headless Chrome driver."""
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        self.driver = webdriver.Chrome(options=options)

    async def find_registration_page(self, base_url: str, doc_url: str = None) -> Optional[str]:
        """
        Navigate to find the API key registration page.

        Uses LLM to analyze page content and determine next steps.
        """
        pass

    async def analyze_registration_form(self, page_url: str) -> Dict[str, Any]:
        """
        Analyze a registration page to understand the form fields.

        Returns:
            {
                "form_type": "email_only|email_and_name|complex",
                "fields": [{"name": "email", "type": "email", "required": True}, ...],
                "submit_button": "selector",
                "captcha_detected": False,
                "requires_verification": True
            }
        """
        pass

    async def submit_registration(
        self,
        page_url: str,
        email: str,
        form_data: Dict[str, str] = None
    ) -> Dict[str, Any]:
        """
        Submit the API key registration form.

        Returns:
            {
                "success": True/False,
                "message": "Check your email...",
                "api_key": "...",  # If immediately provided
                "next_steps": "..."
            }
        """
        pass

    def close(self):
        """Clean up browser resources."""
        if self.driver:
            self.driver.quit()
```

---

### 4. API Key Agent (`agents/api_key_agent.py`)

The main orchestrating agent that coordinates the API key acquisition process.

```python
class APIKeyAgent:
    """
    Agent for automatically acquiring API keys for data sources.

    Orchestrates:
    1. Checking existing key storage
    2. Finding registration pages
    3. Submitting registration forms
    4. Monitoring email for API keys
    """

    def __init__(self, db_client: MongoClient):
        self.llm = ChatAnthropic(
            model=Config.DISCOVERY_LLM_MODEL,
            api_key=Config.ANTHROPIC_API_KEY,
            temperature=0.1,
        )
        self.key_store = APIKeyStore(db_client)
        self.email_service = ArcadeEmailService()
        self.web_navigator = WebNavigator()

    def run(self, state: DiscoveryState) -> DiscoveryState:
        """
        Main entry point for API key acquisition.

        Args:
            state: Current workflow state with access_documentation

        Returns:
            Updated state with API key or error
        """
        try:
            source_name = state["access_documentation"]["source_name"]
            base_url = state["access_documentation"]["base_url"]
            registration_url = state["access_documentation"]["authentication"].get("registration_url")

            # Step 1: Check if we already have a key
            existing_key = self._check_existing_key(base_url, source_name)
            if existing_key:
                return self._apply_key_to_state(state, existing_key)

            # Step 2: Analyze registration requirements
            registration_info = self._analyze_registration(base_url, registration_url)

            # Step 3: Determine acquisition strategy
            strategy = self._determine_strategy(registration_info)

            # Step 4: Execute acquisition
            if strategy == "immediate_registration":
                return self._execute_immediate_registration(state, registration_info)
            elif strategy == "email_verification":
                return self._execute_email_verification_flow(state, registration_info)
            elif strategy == "manual_required":
                return self._request_manual_intervention(state, registration_info)

        except Exception as e:
            logger.error(f"API Key Agent failed: {e}")
            state["error"] = WorkflowError(
                agent_name="APIKeyAgent",
                step="api_key_acquisition",
                issue="Failed to acquire API key",
                details=str(e),
                recoverable=True
            ).to_dict()
            state["interrupted"] = True

        return state

    def _check_existing_key(self, base_url: str, source_name: str) -> Optional[str]:
        """Check if we already have a valid key for this source."""
        # Normalize the identifier
        identifier = self._normalize_source_identifier(base_url)

        if self.key_store.key_exists(identifier):
            key = self.key_store.get_key(identifier)
            if key:
                logger.info(f"Found existing API key for {source_name}")
                return key
        return None

    def _analyze_registration(self, base_url: str, registration_url: str) -> Dict[str, Any]:
        """
        Use LLM to analyze the registration process.

        Returns comprehensive info about how to register.
        """
        pass

    def _determine_strategy(self, registration_info: Dict) -> str:
        """
        Determine the best strategy for acquiring the API key.

        Returns one of:
        - "immediate_registration": Form can be filled and submitted
        - "email_verification": Need to submit form, then check email
        - "manual_required": Too complex, need human intervention
        """
        pass

    async def _execute_email_verification_flow(
        self,
        state: DiscoveryState,
        registration_info: Dict
    ) -> DiscoveryState:
        """
        Execute the email verification flow:
        1. Submit registration form with DISCOVERY_EMAIL
        2. Poll email for API key
        3. Extract and validate key
        """
        source_domain = self._extract_domain(registration_info["url"])

        # Submit registration
        submit_result = await self.web_navigator.submit_registration(
            page_url=registration_info["registration_url"],
            email=Config.DISCOVERY_EMAIL,
            form_data=registration_info.get("additional_fields")
        )

        if not submit_result["success"]:
            # Registration failed
            return self._handle_registration_failure(state, submit_result)

        # Check if key was immediately provided
        if submit_result.get("api_key"):
            return self._apply_key_to_state(state, submit_result["api_key"])

        # Poll email for API key
        api_key = await self._poll_email_for_key(
            source_domain=source_domain,
            subject_keywords=["api key", "API", "access key", "registration"],
            max_attempts=Config.API_KEY_CHECK_MAX_ATTEMPTS,
            interval=Config.API_KEY_CHECK_INTERVAL
        )

        if api_key:
            # Store the key for future use
            self.key_store.store_key(
                source_identifier=self._normalize_source_identifier(registration_info["url"]),
                api_key=api_key,
                metadata={
                    "source_name": state["access_documentation"]["source_name"],
                    "registration_url": registration_info["registration_url"],
                    "registration_email": Config.DISCOVERY_EMAIL,
                }
            )
            return self._apply_key_to_state(state, api_key)

        # Couldn't get key - fall back to manual
        return self._request_manual_intervention(state, {
            "reason": "email_timeout",
            "message": "API key email not received within timeout period"
        })

    async def _poll_email_for_key(
        self,
        source_domain: str,
        subject_keywords: List[str],
        max_attempts: int,
        interval: int
    ) -> Optional[str]:
        """
        Poll email looking for API key.

        Args:
            source_domain: Domain to filter sender
            subject_keywords: Keywords to search in subject
            max_attempts: Maximum polling attempts
            interval: Seconds between attempts

        Returns:
            API key if found, None otherwise
        """
        for attempt in range(max_attempts):
            logger.info(f"Checking email for API key (attempt {attempt + 1}/{max_attempts})")

            emails = await self.email_service.search_for_api_key(
                sender_domain=source_domain,
                subject_keywords=subject_keywords,
                since_minutes=max(30, (attempt + 1) * interval // 60 + 5)
            )

            if emails and emails.get("result"):
                for email in emails["result"]:
                    api_key = await self.email_service.extract_api_key_from_email(
                        email.get("body", "")
                    )
                    if api_key:
                        logger.info("API key found in email!")
                        return api_key

            if attempt < max_attempts - 1:
                await asyncio.sleep(interval)

        return None

    def _apply_key_to_state(self, state: DiscoveryState, api_key: str) -> DiscoveryState:
        """Apply the acquired API key to the workflow state."""
        access_doc = state.get("access_documentation", {})
        access_doc["_provided_api_key"] = api_key
        state["access_documentation"] = access_doc
        state["api_key_acquired"] = True
        state["api_key_source"] = "automatic"
        return state

    def _request_manual_intervention(
        self,
        state: DiscoveryState,
        reason_info: Dict
    ) -> DiscoveryState:
        """Fall back to manual API key input."""
        state["waiting_for_human_input"] = True
        state["pause_reason"] = "needs_api_key"
        state["human_input_request"] = HumanInputRequest(
            input_type=HumanInputType.API_KEY,
            field_name="api_key",
            description=f"Automatic API key acquisition failed: {reason_info.get('message', 'Unknown reason')}. Please provide the API key manually.",
            required=True,
            registration_url=reason_info.get("registration_url"),
            additional_info={
                "automatic_acquisition_attempted": True,
                "failure_reason": reason_info.get("reason"),
            }
        ).to_dict()
        state["interrupted"] = True
        return state
```

---

### 5. State Updates (`state.py`)

Add new state fields for API key acquisition:

```python
class DiscoveryState(TypedDict, total=False):
    # ... existing fields ...

    # API Key Acquisition fields
    api_key_acquired: bool
    api_key_source: str  # "automatic", "existing", "manual"
    api_key_acquisition_attempted: bool
    api_key_acquisition_error: Optional[str]
    email_check_attempts: int
```

Add new HumanInputType for API key scenarios:

```python
class HumanInputType(str, Enum):
    # ... existing types ...
    API_KEY_VERIFICATION = "api_key_verification"  # For email verification links
```

---

### 6. Workflow Integration (`workflow.py`)

Modify the workflow to integrate the API Key Agent:

```python
class DataSourceDiscoveryWorkflow:
    def __init__(self, ...):
        # ... existing agents ...
        self.api_key_agent = APIKeyAgent(db_client)

    def _build_graph(self) -> StateGraph:
        # Add new node
        workflow.add_node("acquire_api_key", self._acquire_api_key_node)

        # Modify edge from "test" to check if API key needed
        workflow.add_conditional_edges(
            "test",
            self._check_api_key_needed,
            {
                "needs_key": "acquire_api_key",
                "continue": "configure",
                "stop": END,
            }
        )

        # Add edge from acquire_api_key back to test
        workflow.add_conditional_edges(
            "acquire_api_key",
            self._check_interrupted,
            {
                "continue": "test",  # Retry test with new key
                "stop": END,
            }
        )

    def _check_api_key_needed(self, state: DiscoveryState) -> str:
        """Check if we need to acquire an API key."""
        if state.get("interrupted"):
            error = state.get("error", {})
            if "authentication" in error.get("issue", "").lower():
                if not state.get("api_key_acquisition_attempted"):
                    return "needs_key"
            return "stop"
        if state.get("waiting_for_human_input"):
            return "stop"
        return "continue"

    def _acquire_api_key_node(self, state: DiscoveryState) -> DiscoveryState:
        """Execute the API key acquisition agent."""
        logger.info("Workflow: Entering API key acquisition node")
        state["api_key_acquisition_attempted"] = True
        state = self.api_key_agent.run(state)
        self._persist_state(state, "acquire_api_key")
        return state
```

---

## Workflow Diagram

```
                    ┌─────────────┐
                    │   Search    │
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │   Examine   │
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │   Select    │
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │  Document   │
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
              ┌────►│    Test     │◄─────────────┐
              │     └──────┬──────┘              │
              │            │                     │
              │     ┌──────▼──────┐              │
              │     │ Auth Error? │              │
              │     └──────┬──────┘              │
              │            │                     │
              │       yes  │  no                 │
              │    ┌───────┴───────┐             │
              │    │               │             │
              │ ┌──▼────────────┐  │             │
              │ │ API Key Agent │──┼─────────────┘
              │ └───────┬───────┘  │    (retry with key)
              │         │          │
              │  ┌──────▼──────┐   │
              │  │ Check Store │   │
              │  └──────┬──────┘   │
              │         │          │
              │    found│  not found
              │         │          │
              │         │   ┌──────▼──────┐
              │         │   │  Navigate   │
              │         │   │   to Reg    │
              │         │   └──────┬──────┘
              │         │          │
              │         │   ┌──────▼──────┐
              │         │   │   Submit    │
              │         │   │    Form     │
              │         │   └──────┬──────┘
              │         │          │
              │         │   ┌──────▼──────┐
              │         │   │ Poll Email  │
              │         │   │ (via Arcade)│
              │         │   └──────┬──────┘
              │         │          │
              │  ┌──────▼──────────▼─┐
              │  │   Key Acquired?   │
              │  └─────────┬─────────┘
              │            │
              │       yes  │  no (timeout)
              │            │
              │            │  ┌────────────────┐
              │            │  │ Request Manual │
              │            │  │     Input      │
              │            │  └────────┬───────┘
              │            │           │
              │            │           ▼
              │            │        PAUSED
              │            │
              │     ┌──────▼──────┐
              └─────│  Configure  │
                    └─────────────┘
```

---

## LLM Prompts

### Registration Analysis Prompt

```python
REGISTRATION_ANALYSIS_PROMPT = """You are analyzing a data source website to determine how to obtain an API key.

Given the following page content from {source_name}:

{page_content}

Analyze and return a JSON object with:
{{
    "registration_type": "form|link|email_request|oauth|none",
    "registration_url": "URL to registration page if found",
    "form_fields": [
        {{"name": "field_name", "type": "text|email|select", "required": true/false, "label": "..."}}
    ],
    "has_captcha": true/false,
    "requires_email_verification": true/false,
    "provides_instant_key": true/false,
    "instructions": "Human-readable steps to get API key",
    "automated_possible": true/false,
    "automation_notes": "Any notes about automating this process"
}}
"""
```

### Email API Key Extraction Prompt

```python
API_KEY_EXTRACTION_PROMPT = """Extract the API key from this email content.

Email content:
{email_content}

Look for:
- Explicit API key labels ("Your API key:", "API Key:", "Access Key:")
- Long alphanumeric strings (typically 20-64 characters)
- Keys in code blocks or highlighted text

Return JSON:
{{
    "api_key_found": true/false,
    "api_key": "the-extracted-key" or null,
    "confidence": 0.0-1.0,
    "extraction_note": "How you identified the key"
}}
"""
```

---

## Dependencies

Add to `requirements.txt`:

```
arcadepy>=0.1.0
selenium>=4.15.0
webdriver-manager>=4.0.0
cryptography>=41.0.0  # For API key encryption
```

---

## Configuration Example

`.env` additions:

```env
# API Key Gathering
ARCADE_API_KEY=your_arcade_api_key
DISCOVERY_EMAIL=discovery@yourcompany.com
ARCADE_USER_ID=discovery@yourcompany.com
API_KEY_CHECK_INTERVAL=30
API_KEY_CHECK_MAX_ATTEMPTS=20
```

---

## Error Handling & Edge Cases

### Handled Scenarios

1. **Existing Key Available**: Skip registration, use stored key
2. **Simple Email Registration**: Submit form, poll email, extract key
3. **Instant Key Provision**: Some APIs give key immediately after form submit
4. **CAPTCHA Detected**: Fall back to manual intervention
5. **OAuth Required**: Fall back to manual (future: could support OAuth flow)
6. **Email Timeout**: After max attempts, fall back to manual
7. **Invalid Key After Extraction**: Re-enter polling or request manual
8. **Selenium Failures**: Graceful degradation to manual

### Not Handled (Manual Required)

1. Phone verification
2. Payment/credit card required
3. Company verification/approval process
4. Multi-factor authentication
5. API key requires additional scopes/permissions setup

---

## Testing Strategy

### Unit Tests

```python
# tests/test_api_key_agent.py

class TestAPIKeyStore:
    def test_store_and_retrieve_key(self):
        """Test storing and retrieving an API key."""
        pass

    def test_key_encryption(self):
        """Test that keys are stored encrypted."""
        pass

    def test_invalidate_key(self):
        """Test marking a key as invalid."""
        pass

class TestArcadeEmailService:
    def test_search_for_api_key(self):
        """Test email search with mock Arcade client."""
        pass

    def test_extract_api_key_from_email(self):
        """Test API key extraction from various email formats."""
        pass

class TestAPIKeyAgent:
    def test_existing_key_flow(self):
        """Test flow when key already exists."""
        pass

    def test_registration_flow(self):
        """Test full registration and email polling flow."""
        pass

    def test_fallback_to_manual(self):
        """Test fallback when automation fails."""
        pass
```

### Integration Tests

```python
# tests/test_api_key_integration.py

class TestAPIKeyWorkflowIntegration:
    def test_workflow_with_api_key_agent(self):
        """Test complete workflow including API key acquisition."""
        pass

    def test_workflow_retry_after_key_acquisition(self):
        """Test that test step is retried after getting key."""
        pass
```

---

## Implementation Order

1. **Phase 1: Core Infrastructure**
   - [ ] Add environment variables to `config.py`
   - [ ] Create `api_key_store.py` with MongoDB storage
   - [ ] Create basic `arcade_email_service.py` wrapper

2. **Phase 2: API Key Agent (Basic)**
   - [ ] Create `api_key_agent.py` with key checking logic
   - [ ] Implement state updates in `state.py`
   - [ ] Integrate agent into `workflow.py`

3. **Phase 3: Email Polling**
   - [ ] Complete `arcade_email_service.py` with search/extract
   - [ ] Add LLM-based key extraction
   - [ ] Implement polling loop

4. **Phase 4: Web Navigation**
   - [ ] Create `web_navigator.py` with Selenium
   - [ ] Implement registration page analysis
   - [ ] Implement form submission

5. **Phase 5: Testing & Refinement**
   - [ ] Unit tests for all components
   - [ ] Integration tests
   - [ ] End-to-end testing with real APIs

---

## Security Considerations

1. **API Key Storage**: Keys stored encrypted using `cryptography` library
2. **Environment Variables**: Sensitive values only in env vars, not code
3. **Email Access**: Arcade handles OAuth securely, tokens managed externally
4. **Selenium**: Headless mode, no persistent sessions
5. **Audit Logging**: Log key acquisition events (without the keys themselves)

---

## Future Enhancements

1. **OAuth Support**: Handle OAuth-based API authentication
2. **Key Rotation**: Automatic key rotation before expiration
3. **Multi-account Support**: Different email accounts for different source types
4. **Browser Extension**: Alternative to Selenium for complex sites
5. **Webhook Support**: Some APIs support webhooks for key delivery

---

## References

- [Arcade API Documentation](https://docs.arcade.dev/en/home)
- [Arcade Python SDK](https://github.com/ArcadeAI/arcade-py)
- [Gmail Tools](https://blog.arcade.dev/build-ai-agents-with-gmail-and-hubspot-authentication-using-arcade)
- Existing codebase: `core/discovery/agents/testing_agent.py`

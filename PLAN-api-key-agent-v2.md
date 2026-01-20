# API Key Gathering Agent - Updated Implementation Plan

## Executive Summary

This plan outlines the implementation of an automated API key gathering system that integrates with the existing discovery workflow. When the Testing Agent detects that an API key is required, the system will:

1. Check for existing stored API keys
2. Use Arcade API to monitor Gmail for API keys received via email
3. Optionally use Selenium to navigate registration pages and submit forms
4. Fall back to manual input if automation fails

This plan incorporates research on Arcade API capabilities and builds upon the existing PLAN-api-key-agent.md.

---

## Key Updates from Original Plan

### Arcade API Insights

Based on research into Arcade's actual API:

1. **Authentication**: Uses Bearer token authentication (`Authorization: Bearer <ARCADE_API_KEY>`)
2. **Tool Execution**: Primary endpoint is `POST /v1/tools/execute` with parameters:
   - `tool_name`: e.g., "Gmail.SearchEmails", "Gmail.ListEmails"
   - `user_id`: Email address of the user (DISCOVERY_EMAIL)
   - `input`: JSON parameters for the tool
3. **Gmail Tools Available**:
   - `Gmail.ListEmails` - List recent emails
   - `Gmail.SearchEmails` - Search with Gmail query syntax
   - `Gmail.GetEmail` - Get full email content
   - `Gmail.SendEmail` - Send emails
4. **Authorization Flow**:
   - If user hasn't authorized Gmail access, API returns `AuthorizationError` with auth URL
   - User must authorize once, then tokens are managed by Arcade
5. **Response Format**: Returns `{"status": "...", "output": {...}, "success": bool}`

### Implementation Adjustments

1. **Email Service**: Will use `arcadepy` Python client instead of direct API calls
2. **Gmail Search**: Use Gmail query syntax (e.g., `from:*@usda.gov subject:(API Key) newer_than:30m`)
3. **Authorization Handling**: Need one-time Gmail authorization before system can work
4. **LLM for Orchestration**: Use Claude to make decisions about extraction and next steps

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    Discovery Workflow                        │
│                                                              │
│  Search → Examine → Select → Document → Test                │
│                                              │               │
│                                              ▼               │
│                                        Auth Failed?          │
│                                              │               │
│                                          ┌───┴───┐           │
│                                          │       │           │
│                                         YES     NO           │
│                                          │       │           │
│                                          ▼       ▼           │
│                                    API Key  Configure        │
│                                     Agent                    │
│                                       │                      │
│                                       ▼                      │
│                               ┌───────────────┐              │
│                               │ Check Store   │              │
│                               └───────┬───────┘              │
│                                       │                      │
│                                  ┌────┴────┐                 │
│                                Found    Not Found            │
│                                  │         │                 │
│                                  │         ▼                 │
│                                  │   ┌──────────────┐        │
│                                  │   │ Web Analysis │        │
│                                  │   │  (LLM + Web) │        │
│                                  │   └──────┬───────┘        │
│                                  │          │                │
│                                  │          ▼                │
│                                  │   ┌──────────────┐        │
│                                  │   │ Registration │        │
│                                  │   │   (Optional  │        │
│                                  │   │   Selenium)  │        │
│                                  │   └──────┬───────┘        │
│                                  │          │                │
│                                  │          ▼                │
│                                  │   ┌──────────────┐        │
│                                  │   │ Email Polling│        │
│                                  │   │ (Arcade API) │        │
│                                  │   └──────┬───────┘        │
│                                  │          │                │
│                                  │     ┌────┴────┐           │
│                                  │  Success   Timeout        │
│                                  │     │          │           │
│                                  ▼     ▼          ▼           │
│                              Apply Key    Request Manual     │
│                                  │             │             │
│                                  ▼             ▼             │
│                             Retry Test    PAUSE              │
└─────────────────────────────────────────────────────────────┘
```

---

## Component Implementation Details

### 1. API Key Store Service

**File**: `core/discovery/services/api_key_store.py`

```python
from typing import Optional, Dict, Any
from datetime import datetime
from pymongo import MongoClient
from cryptography.fernet import Fernet
import os
import logging

logger = logging.getLogger(__name__)

class APIKeyStore:
    """
    Manages encrypted storage and retrieval of API keys in MongoDB.
    """

    def __init__(self, db_client: MongoClient):
        from config import Config
        self.collection = db_client[Config.DATABASE_NAME]["api_keys"]

        # Initialize encryption key from environment or generate
        encryption_key = os.getenv("API_KEY_ENCRYPTION_KEY")
        if not encryption_key:
            logger.warning("API_KEY_ENCRYPTION_KEY not set, generating new key")
            encryption_key = Fernet.generate_key().decode()

        self.cipher = Fernet(encryption_key.encode() if isinstance(encryption_key, str) else encryption_key)

        # Create indexes
        self.collection.create_index("source_identifier", unique=True)
        self.collection.create_index("status")

    def _normalize_identifier(self, identifier: str) -> str:
        """Normalize source identifier (URL or name) to consistent format."""
        from urllib.parse import urlparse

        # If it's a URL, extract domain
        if identifier.startswith("http"):
            parsed = urlparse(identifier)
            return parsed.netloc.lower()

        return identifier.lower().strip()

    def get_key(self, source_identifier: str) -> Optional[str]:
        """
        Retrieve API key for a data source.

        Args:
            source_identifier: Data source URL or normalized name

        Returns:
            Decrypted API key or None if not found or invalid
        """
        identifier = self._normalize_identifier(source_identifier)

        record = self.collection.find_one({
            "source_identifier": identifier,
            "status": "active"
        })

        if not record:
            return None

        try:
            encrypted_key = record["api_key_encrypted"]
            decrypted = self.cipher.decrypt(encrypted_key.encode()).decode()
            logger.info(f"Retrieved API key for {identifier}")
            return decrypted
        except Exception as e:
            logger.error(f"Failed to decrypt API key for {identifier}: {e}")
            return None

    def store_key(
        self,
        source_identifier: str,
        api_key: str,
        metadata: Dict[str, Any]
    ) -> bool:
        """
        Store API key for a data source.

        Args:
            source_identifier: Data source URL or normalized name
            api_key: The API key to store
            metadata: Additional info (registration date, source name, etc.)

        Returns:
            True if stored successfully
        """
        identifier = self._normalize_identifier(source_identifier)

        try:
            encrypted_key = self.cipher.encrypt(api_key.encode()).decode()

            record = {
                "source_identifier": identifier,
                "source_name": metadata.get("source_name", identifier),
                "api_key_encrypted": encrypted_key,
                "registration_email": metadata.get("registration_email"),
                "registration_url": metadata.get("registration_url"),
                "acquired_at": datetime.utcnow(),
                "last_validated": datetime.utcnow(),
                "status": "active",
                "metadata": metadata
            }

            self.collection.update_one(
                {"source_identifier": identifier},
                {"$set": record},
                upsert=True
            )

            logger.info(f"Stored API key for {identifier}")
            return True

        except Exception as e:
            logger.error(f"Failed to store API key for {identifier}: {e}")
            return False

    def key_exists(self, source_identifier: str) -> bool:
        """Check if we have an active key for this source."""
        identifier = self._normalize_identifier(source_identifier)

        count = self.collection.count_documents({
            "source_identifier": identifier,
            "status": "active"
        })

        return count > 0

    def invalidate_key(self, source_identifier: str) -> bool:
        """Mark a key as invalid (e.g., after auth failure)."""
        identifier = self._normalize_identifier(source_identifier)

        result = self.collection.update_one(
            {"source_identifier": identifier},
            {"$set": {"status": "invalid", "invalidated_at": datetime.utcnow()}}
        )

        if result.modified_count > 0:
            logger.info(f"Invalidated API key for {identifier}")
            return True

        return False
```

---

### 2. Arcade Email Service

**File**: `core/discovery/services/arcade_email_service.py`

```python
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
import logging
from arcadepy import Arcade
import json

logger = logging.getLogger(__name__)

class ArcadeEmailService:
    """
    Service for checking email via Arcade API.
    Used to retrieve API keys sent via email after registration.
    """

    def __init__(self, api_key: str, user_id: str):
        """
        Initialize Arcade email service.

        Args:
            api_key: Arcade API key
            user_id: User identifier (typically email address)
        """
        self.client = Arcade(api_key=api_key)
        self.user_id = user_id

    def authorize_gmail(self) -> Dict[str, Any]:
        """
        Check Gmail authorization status.

        Returns:
            Authorization info or URL if consent needed
        """
        try:
            # Try to execute a simple tool to check authorization
            result = self.client.tools.execute(
                tool_name="Gmail.ListEmails",
                input={"n_emails": 1},
                user_id=self.user_id
            )
            logger.info("Gmail already authorized")
            return {"authorized": True}

        except Exception as e:
            error_msg = str(e)
            if "authorization" in error_msg.lower():
                logger.warning("Gmail authorization required")
                # Extract auth URL from error if available
                return {
                    "authorized": False,
                    "error": error_msg,
                    "message": "Gmail authorization required. Please authorize via Arcade dashboard."
                }
            raise

    def search_for_api_key(
        self,
        sender_domain: str,
        subject_keywords: List[str],
        since_minutes: int = 30
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Search for API key email from a data source.

        Args:
            sender_domain: Email domain of the data source (e.g., "usda.gov")
            subject_keywords: Keywords to search for in subject
            since_minutes: Only check emails from last N minutes

        Returns:
            List of matching emails or None
        """
        try:
            # Build Gmail search query
            query_parts = []

            # Filter by sender domain
            if sender_domain:
                query_parts.append(f"from:*@{sender_domain}")

            # Filter by subject keywords
            if subject_keywords:
                # Gmail OR syntax for multiple keywords
                subject_query = " OR ".join(subject_keywords)
                query_parts.append(f"subject:({subject_query})")

            # Filter by time
            if since_minutes:
                query_parts.append(f"newer_than:{since_minutes}m")

            query = " ".join(query_parts)

            logger.info(f"Searching Gmail with query: {query}")

            result = self.client.tools.execute(
                tool_name="Gmail.SearchEmails",
                input={
                    "query": query,
                    "max_results": 10
                },
                user_id=self.user_id
            )

            if result.output and isinstance(result.output, dict):
                emails = result.output.get("emails", [])
                logger.info(f"Found {len(emails)} matching emails")
                return emails

            return None

        except Exception as e:
            logger.error(f"Failed to search Gmail: {e}")
            return None

    def get_email_content(self, email_id: str) -> Optional[Dict[str, Any]]:
        """
        Get full email content by ID.

        Args:
            email_id: Gmail message ID

        Returns:
            Email content dict or None
        """
        try:
            result = self.client.tools.execute(
                tool_name="Gmail.GetEmail",
                input={"email_id": email_id},
                user_id=self.user_id
            )

            if result.output:
                return result.output

            return None

        except Exception as e:
            logger.error(f"Failed to get email content: {e}")
            return None

    def extract_api_key_from_email(
        self,
        email_content: str,
        llm: Any
    ) -> Optional[str]:
        """
        Use LLM to extract API key from email content.

        Args:
            email_content: Email body text
            llm: LLM instance for extraction

        Returns:
            Extracted API key or None
        """
        from langchain_core.messages import HumanMessage

        prompt = f"""Extract the API key from this email content.

Email content:
{email_content}

Look for:
- Explicit API key labels ("Your API key:", "API Key:", "Access Key:", "Token:")
- Long alphanumeric strings (typically 20-64 characters)
- Keys in code blocks or highlighted text
- UUID format keys
- Base64 encoded strings

Return ONLY the API key string if found, or "NOT_FOUND" if no key is present.
Do not include any explanation, just the key or NOT_FOUND.
"""

        try:
            response = llm.invoke([HumanMessage(content=prompt)])
            extracted = response.content.strip()

            if extracted and extracted != "NOT_FOUND" and len(extracted) > 10:
                logger.info(f"Extracted API key from email (length: {len(extracted)})")
                return extracted

            logger.warning("No API key found in email")
            return None

        except Exception as e:
            logger.error(f"Failed to extract API key: {e}")
            return None
```

---

### 3. Web Navigator Service (Optional - Selenium)

**File**: `core/discovery/services/web_navigator.py`

```python
from typing import Optional, Dict, Any
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

logger = logging.getLogger(__name__)

class WebNavigator:
    """
    Selenium-based web navigator for API key registration.
    Optional component - only used when registration automation is needed.
    """

    def __init__(self, llm: Any):
        """
        Initialize web navigator.

        Args:
            llm: LLM instance for page analysis
        """
        self.driver = None
        self.llm = llm

    def _init_driver(self):
        """Initialize headless Chrome driver."""
        if self.driver:
            return

        try:
            options = webdriver.ChromeOptions()
            options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu')
            options.add_argument('--window-size=1920,1080')

            self.driver = webdriver.Chrome(options=options)
            logger.info("Initialized Chrome WebDriver")

        except Exception as e:
            logger.error(f"Failed to initialize WebDriver: {e}")
            raise

    def navigate_to_registration(self, registration_url: str) -> bool:
        """
        Navigate to registration page.

        Args:
            registration_url: URL of registration page

        Returns:
            True if navigation successful
        """
        self._init_driver()

        try:
            self.driver.get(registration_url)
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            logger.info(f"Navigated to {registration_url}")
            return True

        except Exception as e:
            logger.error(f"Failed to navigate to registration page: {e}")
            return False

    def analyze_registration_form(self) -> Dict[str, Any]:
        """
        Analyze registration page to understand form fields.

        Returns:
            Form analysis dict
        """
        if not self.driver:
            return {"error": "Driver not initialized"}

        try:
            # Get page HTML
            page_source = self.driver.page_source
            page_text = self.driver.find_element(By.TAG_NAME, "body").text

            # Use LLM to analyze
            from langchain_core.messages import HumanMessage

            prompt = f"""Analyze this registration page to determine if API key registration can be automated.

Page text:
{page_text[:2000]}

Determine:
1. Is there a registration form?
2. What fields are required (email, name, etc.)?
3. Is there a CAPTCHA?
4. Does it require manual approval?
5. Can this be automated with Selenium?

Return JSON:
{{
    "has_form": true/false,
    "automatable": true/false,
    "required_fields": ["email", "name", ...],
    "has_captcha": true/false,
    "complexity": "simple|moderate|complex",
    "notes": "..."
}}
"""

            response = self.llm.invoke([HumanMessage(content=prompt)])
            import json

            # Try to extract JSON from response
            content = response.content
            if "```json" in content:
                content = content.split("```json")[1].split("```")[0]
            elif "```" in content:
                content = content.split("```")[1].split("```")[0]

            analysis = json.loads(content.strip())
            logger.info(f"Form analysis: {analysis}")
            return analysis

        except Exception as e:
            logger.error(f"Failed to analyze form: {e}")
            return {
                "has_form": False,
                "automatable": False,
                "error": str(e)
            }

    def submit_registration(
        self,
        email: str,
        form_data: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Submit registration form.

        Args:
            email: Email address to register
            form_data: Additional form field values

        Returns:
            Submission result dict
        """
        if not self.driver:
            return {"success": False, "error": "Driver not initialized"}

        try:
            # Find email input field
            email_input = None
            for selector in ["input[type='email']", "input[name*='email']", "input[id*='email']"]:
                try:
                    email_input = self.driver.find_element(By.CSS_SELECTOR, selector)
                    break
                except:
                    continue

            if not email_input:
                return {"success": False, "error": "Could not find email input field"}

            # Fill email
            email_input.clear()
            email_input.send_keys(email)
            logger.info("Filled email field")

            # Fill additional fields if provided
            if form_data:
                for field_name, value in form_data.items():
                    try:
                        field = self.driver.find_element(By.NAME, field_name)
                        field.clear()
                        field.send_keys(value)
                    except:
                        logger.warning(f"Could not fill field: {field_name}")

            # Find and click submit button
            submit_button = None
            for selector in [
                "button[type='submit']",
                "input[type='submit']",
                "button:contains('Submit')",
                "button:contains('Register')",
                "button:contains('Sign')"
            ]:
                try:
                    submit_button = self.driver.find_element(By.CSS_SELECTOR, selector)
                    break
                except:
                    continue

            if not submit_button:
                return {"success": False, "error": "Could not find submit button"}

            # Click submit
            submit_button.click()
            logger.info("Clicked submit button")

            # Wait for response
            import time
            time.sleep(3)

            # Check for success message
            page_text = self.driver.find_element(By.TAG_NAME, "body").text.lower()

            if any(keyword in page_text for keyword in ["success", "check your email", "sent", "registered"]):
                return {
                    "success": True,
                    "message": "Registration submitted successfully"
                }
            else:
                return {
                    "success": False,
                    "error": "Uncertain if registration succeeded",
                    "page_text": page_text[:500]
                }

        except Exception as e:
            logger.error(f"Failed to submit registration: {e}")
            return {"success": False, "error": str(e)}

    def close(self):
        """Clean up browser resources."""
        if self.driver:
            try:
                self.driver.quit()
                logger.info("Closed WebDriver")
            except:
                pass
            finally:
                self.driver = None
```

---

### 4. API Key Agent

**File**: `core/discovery/agents/api_key_agent.py`

```python
from typing import Optional, Dict, Any
import logging
import asyncio
from pymongo import MongoClient
from langchain_anthropic import ChatAnthropic

from core.discovery.state import DiscoveryState, HumanInputRequest, HumanInputType, WorkflowError
from core.discovery.services.api_key_store import APIKeyStore
from core.discovery.services.arcade_email_service import ArcadeEmailService
from core.discovery.services.web_navigator import WebNavigator
from config import Config

logger = logging.getLogger(__name__)

class APIKeyAgent:
    """
    Agent for automatically acquiring API keys for data sources.

    Orchestrates:
    1. Checking existing key storage
    2. Finding registration pages
    3. Optionally submitting registration forms (Selenium)
    4. Monitoring email for API keys (Arcade)
    """

    def __init__(self, db_client: MongoClient):
        self.llm = ChatAnthropic(
            model=Config.DISCOVERY_LLM_MODEL,
            api_key=Config.ANTHROPIC_API_KEY,
            temperature=0.1,
        )
        self.key_store = APIKeyStore(db_client)

        # Initialize Arcade email service if configured
        if Config.ARCADE_API_KEY and Config.DISCOVERY_EMAIL:
            self.email_service = ArcadeEmailService(
                api_key=Config.ARCADE_API_KEY,
                user_id=Config.DISCOVERY_EMAIL
            )
        else:
            self.email_service = None
            logger.warning("Arcade API not configured, email polling disabled")

        self.web_navigator = None  # Lazy initialization

    def run(self, state: DiscoveryState) -> DiscoveryState:
        """
        Main entry point for API key acquisition.

        Args:
            state: Current workflow state with access_documentation

        Returns:
            Updated state with API key or error
        """
        try:
            access_doc = state.get("access_documentation", {})
            source_name = access_doc.get("source_name", "Unknown")
            base_url = access_doc.get("base_url", "")
            auth_info = access_doc.get("authentication", {})
            registration_url = auth_info.get("registration_url", "")

            logger.info(f"API Key Agent: Processing {source_name}")

            # Step 1: Check if we already have a key
            existing_key = self._check_existing_key(base_url, source_name)
            if existing_key:
                logger.info(f"Using existing API key for {source_name}")
                return self._apply_key_to_state(state, existing_key, "existing")

            # Step 2: Determine if we can attempt automatic acquisition
            if not self.email_service:
                logger.info("Email service not configured, requesting manual input")
                return self._request_manual_intervention(state, {
                    "reason": "no_email_service",
                    "message": "Arcade API not configured for automatic key acquisition",
                    "registration_url": registration_url
                })

            # Step 3: Check Gmail authorization
            auth_status = self.email_service.authorize_gmail()
            if not auth_status.get("authorized"):
                logger.warning("Gmail not authorized")
                return self._request_manual_intervention(state, {
                    "reason": "gmail_not_authorized",
                    "message": auth_status.get("message", "Gmail authorization required"),
                    "registration_url": registration_url
                })

            # Step 4: Analyze registration process
            registration_info = self._analyze_registration(
                source_name=source_name,
                base_url=base_url,
                registration_url=registration_url,
                auth_info=auth_info
            )

            # Step 5: Determine strategy
            strategy = self._determine_strategy(registration_info)
            logger.info(f"Using strategy: {strategy}")

            # Step 6: Execute based on strategy
            if strategy == "email_only":
                # Just poll email without form submission
                return asyncio.run(self._poll_email_only(state, registration_info))

            elif strategy == "manual_registration_email_check":
                # Ask user to register, then we'll check email
                return self._request_manual_registration_then_poll(state, registration_info)

            elif strategy == "automated_selenium":
                # Use Selenium to submit form, then poll email
                return asyncio.run(self._execute_selenium_flow(state, registration_info))

            else:  # manual_required
                return self._request_manual_intervention(state, registration_info)

        except Exception as e:
            logger.error(f"API Key Agent failed: {e}", exc_info=True)
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
        if not base_url:
            return None

        if self.key_store.key_exists(base_url):
            key = self.key_store.get_key(base_url)
            if key:
                return key

        return None

    def _analyze_registration(
        self,
        source_name: str,
        base_url: str,
        registration_url: str,
        auth_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Analyze the registration process using LLM.

        Returns:
            Registration analysis dict
        """
        from langchain_core.messages import HumanMessage

        prompt = f"""Analyze this API registration information to determine the best approach for automatic API key acquisition.

Data Source: {source_name}
Base URL: {base_url}
Registration URL: {registration_url}
Authentication Info: {auth_info}

Based on this information, determine:

1. **Registration Complexity**: simple|moderate|complex
   - Simple: Just need to check email (key already requested or publicly available)
   - Moderate: Need to fill a form with email, then check email
   - Complex: Requires manual approval, payment, phone verification, etc.

2. **Email Domain**: What domain will send the API key email? (e.g., usda.gov, data.gov)

3. **Subject Keywords**: What keywords would appear in the API key email subject? (e.g., "API Key", "Registration", "Access Token")

4. **Form Automation**: Can a registration form be automated with Selenium?

5. **Recommended Strategy**:
   - "email_only": Just poll email (key may already be sent)
   - "manual_registration_email_check": Ask user to register manually, then we poll email
   - "automated_selenium": Use Selenium to fill form, then poll email
   - "manual_required": Too complex, need full manual process

Return JSON:
{{
    "complexity": "simple|moderate|complex",
    "email_domain": "example.gov",
    "subject_keywords": ["API Key", "Registration"],
    "form_automatable": true/false,
    "recommended_strategy": "...",
    "confidence": 0.0-1.0,
    "notes": "explanation"
}}
"""

        try:
            response = self.llm.invoke([HumanMessage(content=prompt)])
            import json

            content = response.content
            if "```json" in content:
                content = content.split("```json")[1].split("```")[0]
            elif "```" in content:
                content = content.split("```")[1].split("```")[0]

            analysis = json.loads(content.strip())
            analysis["registration_url"] = registration_url
            analysis["base_url"] = base_url
            analysis["source_name"] = source_name

            logger.info(f"Registration analysis: {analysis}")
            return analysis

        except Exception as e:
            logger.error(f"Failed to analyze registration: {e}")
            return {
                "complexity": "complex",
                "recommended_strategy": "manual_required",
                "registration_url": registration_url,
                "error": str(e)
            }

    def _determine_strategy(self, registration_info: Dict[str, Any]) -> str:
        """
        Determine the best strategy for acquiring the API key.

        Returns:
            Strategy name
        """
        recommended = registration_info.get("recommended_strategy", "manual_required")
        complexity = registration_info.get("complexity", "complex")
        confidence = registration_info.get("confidence", 0.0)

        # If LLM has high confidence, use its recommendation
        if confidence > 0.7:
            return recommended

        # Otherwise, use conservative approach based on complexity
        if complexity == "simple":
            return "email_only"
        elif complexity == "moderate":
            return "manual_registration_email_check"
        else:
            return "manual_required"

    async def _poll_email_only(
        self,
        state: DiscoveryState,
        registration_info: Dict[str, Any]
    ) -> DiscoveryState:
        """
        Just poll email for API key (assume registration already done).
        """
        email_domain = registration_info.get("email_domain", "")
        subject_keywords = registration_info.get("subject_keywords", ["API Key", "API", "Token"])

        api_key = await self._poll_email_for_key(
            source_domain=email_domain,
            subject_keywords=subject_keywords,
            max_attempts=Config.API_KEY_CHECK_MAX_ATTEMPTS,
            interval=Config.API_KEY_CHECK_INTERVAL
        )

        if api_key:
            self._store_key(registration_info["base_url"], api_key, registration_info)
            return self._apply_key_to_state(state, api_key, "automatic")

        # No key found, ask user to provide
        return self._request_manual_intervention(state, {
            "reason": "email_timeout",
            "message": "No API key found in recent emails. Please register and provide the key manually.",
            "registration_url": registration_info.get("registration_url")
        })

    def _request_manual_registration_then_poll(
        self,
        state: DiscoveryState,
        registration_info: Dict[str, Any]
    ) -> DiscoveryState:
        """
        Ask user to register manually, then we'll poll email.
        """
        # For now, just request manual input
        # In future, could pause workflow, wait for user confirmation, then poll
        return self._request_manual_intervention(state, {
            "reason": "manual_registration_needed",
            "message": f"Please register for an API key at the registration URL. Use email: {Config.DISCOVERY_EMAIL}",
            "registration_url": registration_info.get("registration_url"),
            "instructions": f"After registering, we'll automatically check your email for the API key."
        })

    async def _execute_selenium_flow(
        self,
        state: DiscoveryState,
        registration_info: Dict[str, Any]
    ) -> DiscoveryState:
        """
        Use Selenium to submit registration form, then poll email.
        """
        registration_url = registration_info.get("registration_url")

        if not registration_url:
            return self._request_manual_intervention(state, {
                "reason": "no_registration_url",
                "message": "No registration URL found"
            })

        # Initialize web navigator
        if not self.web_navigator:
            self.web_navigator = WebNavigator(self.llm)

        try:
            # Navigate to registration page
            if not self.web_navigator.navigate_to_registration(registration_url):
                raise Exception("Failed to navigate to registration page")

            # Analyze form
            form_analysis = self.web_navigator.analyze_registration_form()

            if not form_analysis.get("automatable", False):
                self.web_navigator.close()
                return self._request_manual_intervention(state, {
                    "reason": "form_not_automatable",
                    "message": form_analysis.get("notes", "Form too complex for automation"),
                    "registration_url": registration_url
                })

            # Submit form
            submit_result = self.web_navigator.submit_registration(
                email=Config.DISCOVERY_EMAIL,
                form_data=None  # Could add name, organization, etc.
            )

            self.web_navigator.close()

            if not submit_result.get("success"):
                return self._request_manual_intervention(state, {
                    "reason": "registration_failed",
                    "message": submit_result.get("error", "Registration submission failed"),
                    "registration_url": registration_url
                })

            # Poll email for key
            email_domain = registration_info.get("email_domain", "")
            subject_keywords = registration_info.get("subject_keywords", ["API Key"])

            api_key = await self._poll_email_for_key(
                source_domain=email_domain,
                subject_keywords=subject_keywords,
                max_attempts=Config.API_KEY_CHECK_MAX_ATTEMPTS,
                interval=Config.API_KEY_CHECK_INTERVAL
            )

            if api_key:
                self._store_key(registration_info["base_url"], api_key, registration_info)
                return self._apply_key_to_state(state, api_key, "automatic")

            # Timeout
            return self._request_manual_intervention(state, {
                "reason": "email_timeout_after_registration",
                "message": "Registration submitted but API key not received. Please check email and provide manually.",
                "registration_url": registration_url
            })

        except Exception as e:
            logger.error(f"Selenium flow failed: {e}")
            if self.web_navigator:
                self.web_navigator.close()

            return self._request_manual_intervention(state, {
                "reason": "selenium_error",
                "message": f"Automated registration failed: {str(e)}",
                "registration_url": registration_url
            })

    async def _poll_email_for_key(
        self,
        source_domain: str,
        subject_keywords: list,
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
        if not self.email_service:
            return None

        for attempt in range(max_attempts):
            logger.info(f"Checking email for API key (attempt {attempt + 1}/{max_attempts})")

            # Calculate time window (look further back on later attempts)
            since_minutes = max(30, (attempt + 1) * interval // 60 + 5)

            emails = self.email_service.search_for_api_key(
                sender_domain=source_domain,
                subject_keywords=subject_keywords,
                since_minutes=since_minutes
            )

            if emails:
                for email in emails:
                    # Get full email content
                    email_id = email.get("id")
                    if email_id:
                        full_email = self.email_service.get_email_content(email_id)
                        email_body = full_email.get("body", "") if full_email else email.get("snippet", "")
                    else:
                        email_body = email.get("snippet", "") or email.get("body", "")

                    # Try to extract API key
                    api_key = self.email_service.extract_api_key_from_email(
                        email_content=email_body,
                        llm=self.llm
                    )

                    if api_key:
                        logger.info("API key found in email!")
                        return api_key

            # Wait before next attempt
            if attempt < max_attempts - 1:
                logger.info(f"No key found, waiting {interval} seconds...")
                await asyncio.sleep(interval)

        logger.warning("Email polling timeout - no API key found")
        return None

    def _store_key(
        self,
        base_url: str,
        api_key: str,
        registration_info: Dict[str, Any]
    ):
        """Store the acquired API key."""
        self.key_store.store_key(
            source_identifier=base_url,
            api_key=api_key,
            metadata={
                "source_name": registration_info.get("source_name"),
                "registration_url": registration_info.get("registration_url"),
                "registration_email": Config.DISCOVERY_EMAIL,
                "acquisition_method": "automatic"
            }
        )

    def _apply_key_to_state(
        self,
        state: DiscoveryState,
        api_key: str,
        source: str
    ) -> DiscoveryState:
        """Apply the acquired API key to the workflow state."""
        access_doc = state.get("access_documentation", {})
        access_doc["_provided_api_key"] = api_key
        state["access_documentation"] = access_doc
        state["api_key_acquired"] = True
        state["api_key_source"] = source
        state["api_key_acquisition_attempted"] = True

        # Clear any errors or interruptions
        state["interrupted"] = False
        if state.get("error", {}).get("agent_name") == "TestingAgent":
            state.pop("error", None)

        logger.info(f"Applied API key from {source}")
        return state

    def _request_manual_intervention(
        self,
        state: DiscoveryState,
        reason_info: Dict[str, Any]
    ) -> DiscoveryState:
        """Fall back to manual API key input."""
        state["waiting_for_human_input"] = True
        state["pause_reason"] = "needs_api_key"
        state["api_key_acquisition_attempted"] = True

        message = reason_info.get("message", "Please provide the API key manually.")
        registration_url = reason_info.get("registration_url", "")

        state["human_input_request"] = HumanInputRequest(
            input_type=HumanInputType.API_KEY,
            field_name="api_key",
            description=message,
            required=True,
            registration_url=registration_url,
            additional_info={
                "automatic_acquisition_attempted": True,
                "failure_reason": reason_info.get("reason"),
                "instructions": reason_info.get("instructions", f"Please register at: {registration_url}")
            }
        ).to_dict()

        state["interrupted"] = True
        logger.info(f"Requesting manual intervention: {reason_info.get('reason')}")
        return state

    def __del__(self):
        """Cleanup on deletion."""
        if self.web_navigator:
            self.web_navigator.close()
```

---

### 5. State Updates

**File**: `core/discovery/state.py` (modifications)

Add these fields to `DiscoveryState`:

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

---

### 6. Workflow Integration

**File**: `core/discovery/workflow.py` (modifications)

```python
class DataSourceDiscoveryWorkflow:
    def __init__(self, db_client: MongoClient):
        # ... existing initialization ...
        self.api_key_agent = APIKeyAgent(db_client)

    def _build_graph(self) -> StateGraph:
        workflow = StateGraph(DiscoveryState)

        # ... existing nodes ...

        # Add API key agent node
        workflow.add_node("acquire_api_key", self._acquire_api_key_node)

        # ... existing edges ...

        # Modify edge from "test" to check if API key needed
        workflow.add_conditional_edges(
            "test",
            self._check_after_test,
            {
                "needs_key": "acquire_api_key",
                "configure": "configure",
                "stop": END,
            }
        )

        # Add edge from acquire_api_key back to test or stop
        workflow.add_conditional_edges(
            "acquire_api_key",
            self._check_after_key_acquisition,
            {
                "retry_test": "test",  # Retry test with new key
                "stop": END,
            }
        )

        return workflow

    def _check_after_test(self, state: DiscoveryState) -> str:
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
            if any(keyword in error_issue for keyword in ["authentication", "authorization", "401", "403", "api key"]):
                # Only attempt key acquisition once
                if not state.get("api_key_acquisition_attempted"):
                    logger.info("Auth error detected, attempting API key acquisition")
                    return "needs_key"

        # Otherwise stop (will pause for human input or end with error)
        return "stop"

    def _check_after_key_acquisition(self, state: DiscoveryState) -> str:
        """Check what to do after key acquisition attempt."""
        if state.get("api_key_acquired"):
            # Successfully acquired key, retry test
            logger.info("API key acquired, retrying test")
            return "retry_test"
        else:
            # Failed to acquire, stop (will pause for manual input)
            logger.info("API key acquisition failed, pausing for manual input")
            return "stop"

    def _acquire_api_key_node(self, state: DiscoveryState) -> DiscoveryState:
        """Execute the API key acquisition agent."""
        logger.info("Workflow: Entering API key acquisition node")
        state["current_step"] = "acquire_api_key"
        state = self.api_key_agent.run(state)
        self._persist_state(state, "acquire_api_key")
        return state
```

---

### 7. Configuration

**File**: `config.py` (additions)

```python
# API Key Gathering Configuration
ARCADE_API_KEY = os.getenv("ARCADE_API_KEY", "")
DISCOVERY_EMAIL = os.getenv("DISCOVERY_EMAIL", "")
ARCADE_USER_ID = os.getenv("ARCADE_USER_ID", "")  # Optional, defaults to DISCOVERY_EMAIL
API_KEY_CHECK_INTERVAL = int(os.getenv("API_KEY_CHECK_INTERVAL", "30"))  # Seconds
API_KEY_CHECK_MAX_ATTEMPTS = int(os.getenv("API_KEY_CHECK_MAX_ATTEMPTS", "20"))  # Max attempts
API_KEY_ENCRYPTION_KEY = os.getenv("API_KEY_ENCRYPTION_KEY", "")  # Fernet key for encryption
```

---

## Dependencies

**File**: `requirements.txt` (additions)

```txt
# API Key Gathering
arcadepy>=0.1.0
selenium>=4.15.0
webdriver-manager>=4.0.0
cryptography>=41.0.0
```

---

## Environment Setup

**File**: `.env.example` (additions)

```env
# API Key Gathering Configuration
ARCADE_API_KEY=your_arcade_api_key_here
DISCOVERY_EMAIL=discovery@yourdomain.com
ARCADE_USER_ID=  # Optional, defaults to DISCOVERY_EMAIL
API_KEY_CHECK_INTERVAL=30
API_KEY_CHECK_MAX_ATTEMPTS=20
API_KEY_ENCRYPTION_KEY=  # Will be auto-generated if not provided
```

---

## Testing Strategy

### Unit Tests

Create `tests/test_api_key_agent.py`:

```python
import pytest
from core.discovery.services.api_key_store import APIKeyStore
from core.discovery.services.arcade_email_service import ArcadeEmailService
from core.discovery.agents.api_key_agent import APIKeyAgent

class TestAPIKeyStore:
    def test_store_and_retrieve_key(self, mongo_client):
        store = APIKeyStore(mongo_client)
        store.store_key("test.gov", "test-key-123", {"source_name": "Test"})
        retrieved = store.get_key("test.gov")
        assert retrieved == "test-key-123"

    def test_key_encryption(self, mongo_client):
        store = APIKeyStore(mongo_client)
        store.store_key("test.gov", "secret-key", {})

        # Check that stored value is encrypted
        record = store.collection.find_one({"source_identifier": "test.gov"})
        assert record["api_key_encrypted"] != "secret-key"

class TestArcadeEmailService:
    def test_search_for_api_key(self, mock_arcade_client):
        service = ArcadeEmailService("test-key", "test@example.com")
        service.client = mock_arcade_client

        emails = service.search_for_api_key("usda.gov", ["API Key"], 30)
        assert emails is not None

class TestAPIKeyAgent:
    def test_existing_key_flow(self, mongo_client, test_state):
        agent = APIKeyAgent(mongo_client)
        # Pre-populate key
        agent.key_store.store_key("test.gov", "existing-key", {})

        test_state["access_documentation"] = {"base_url": "https://test.gov"}
        result = agent.run(test_state)

        assert result["api_key_acquired"] == True
        assert result["api_key_source"] == "existing"
```

---

## Implementation Phases

### Phase 1: Core Infrastructure (Week 1)
- [ ] Add environment variables to `config.py`
- [ ] Create `services/__init__.py`
- [ ] Implement `api_key_store.py` with encryption
- [ ] Set up MongoDB collection and indexes
- [ ] Write unit tests for key store

### Phase 2: Email Service (Week 1-2)
- [ ] Implement `arcade_email_service.py`
- [ ] Test Gmail authorization flow
- [ ] Implement email search functionality
- [ ] Implement LLM-based key extraction
- [ ] Write unit tests for email service

### Phase 3: API Key Agent Core (Week 2)
- [ ] Create `api_key_agent.py` skeleton
- [ ] Implement key checking logic
- [ ] Implement email polling flow
- [ ] Add state updates to `state.py`
- [ ] Write unit tests

### Phase 4: Workflow Integration (Week 2-3)
- [ ] Integrate agent into `workflow.py`
- [ ] Add conditional edges for key acquisition
- [ ] Test end-to-end flow
- [ ] Handle edge cases

### Phase 5: Web Navigator (Optional - Week 3-4)
- [ ] Implement `web_navigator.py` with Selenium
- [ ] Implement form analysis
- [ ] Implement form submission
- [ ] Test with real registration pages
- [ ] Write tests

### Phase 6: Testing & Refinement (Week 4)
- [ ] Integration tests
- [ ] End-to-end tests with real APIs
- [ ] Performance optimization
- [ ] Documentation
- [ ] Security audit

---

## Security Considerations

1. **API Key Storage**:
   - Keys encrypted using `cryptography.Fernet`
   - Encryption key stored in environment variable
   - MongoDB collection has restricted access

2. **Email Access**:
   - Arcade handles OAuth tokens securely
   - One-time authorization required
   - Tokens managed externally

3. **Selenium**:
   - Headless mode only
   - No persistent sessions
   - No credential storage in browser

4. **Logging**:
   - Never log API keys
   - Log acquisition events with metadata only
   - Audit trail for key usage

---

## Success Criteria

1. **Functional**:
   - [ ] Can retrieve existing API keys from store
   - [ ] Can poll Gmail and extract API keys
   - [ ] Can handle timeout and fall back to manual
   - [ ] Integrates seamlessly with existing workflow

2. **Performance**:
   - [ ] Key retrieval < 100ms
   - [ ] Email polling completes within timeout period
   - [ ] No blocking of main workflow thread

3. **Reliability**:
   - [ ] Handles Arcade API errors gracefully
   - [ ] Handles missing configuration
   - [ ] Proper fallback to manual input
   - [ ] No data loss on failure

4. **Security**:
   - [ ] Keys stored encrypted
   - [ ] No keys in logs
   - [ ] Secure OAuth flow
   - [ ] Audit trail maintained

---

## Future Enhancements

1. **OAuth Support**: Handle OAuth-based API authentication flows
2. **Key Rotation**: Automatic key rotation before expiration
3. **Multi-account**: Support multiple email accounts for different sources
4. **Webhook Support**: Listen for webhooks instead of polling email
5. **Browser Extension**: Alternative to Selenium for complex JavaScript sites
6. **AI Vision**: Use vision models to solve CAPTCHAs (with proper authorization)

---

## References

- [Arcade API Documentation](https://docs.arcade.dev)
- [Arcade Python SDK](https://github.com/ArcadeAI/arcade-py)
- [Gmail Tools Blog Post](https://blog.arcade.dev/how-to-build-an-ai-agent-for-gmail-a-complete-guide-for-2025)
- Existing codebase: `core/discovery/agents/testing_agent.py`
- Previous plan: `PLAN-api-key-agent.md`

---

## Sources

- [Arcade API Reference](https://docs.arcade.dev/en/references/api)
- [Arcade Quickstart Guide](https://docs.arcade.dev/en/home/quickstart)
- [Build AI Agents with Gmail - Arcade Blog](https://blog.arcade.dev/build-ai-agents-with-gmail-and-hubspot-authentication-using-arcade)
- [Arcade Python SDK - GitHub](https://github.com/ArcadeAI/arcade-py)
- [Arcade AI Agent Example - GitHub](https://github.com/coleam00/arcade-ai-agent)
- [Arcade Toolkits Documentation](https://docs.arcade.dev/toolkits)

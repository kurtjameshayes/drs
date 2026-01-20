"""
Arcade Email Service

Service for checking email via Arcade API.
Used to retrieve API keys sent via email after registration.
"""

from typing import Optional, Dict, Any, List
import logging

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
        try:
            from arcadepy import Arcade

            self.client = Arcade(api_key=api_key)
            self.user_id = user_id
            logger.info(f"Arcade Email Service initialized for user: {user_id}")
        except ImportError as e:
            logger.error(
                "arcadepy not installed. Install with: pip install arcadepy"
            )
            raise ImportError(
                "arcadepy package is required for Arcade Email Service. "
                "Install with: pip install arcadepy"
            ) from e

    def authorize_gmail(self) -> Dict[str, Any]:
        """
        Check Gmail authorization status.

        Returns:
            Authorization info or URL if consent needed
        """
        try:
            # Try to execute a simple tool to check authorization
            result = self.client.tools.execute(
                tool_name="Gmail.ListEmails", input={"n_emails": 1}, user_id=self.user_id
            )
            logger.info("Gmail already authorized")
            return {"authorized": True}

        except Exception as e:
            error_msg = str(e)
            if "authorization" in error_msg.lower() or "auth" in error_msg.lower():
                logger.warning("Gmail authorization required")
                return {
                    "authorized": False,
                    "error": error_msg,
                    "message": "Gmail authorization required. Please authorize via Arcade dashboard.",
                }
            # Re-raise if it's not an auth error
            logger.error(f"Unexpected error checking Gmail authorization: {e}")
            raise

    def search_for_api_key(
        self,
        sender_domain: str,
        subject_keywords: List[str],
        since_minutes: int = 30,
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
                input={"query": query, "max_results": 10},
                user_id=self.user_id,
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
                user_id=self.user_id,
            )

            if result.output:
                logger.info(f"Retrieved email content for ID: {email_id}")
                return result.output

            return None

        except Exception as e:
            logger.error(f"Failed to get email content: {e}")
            return None

    def extract_api_key_from_email(
        self, email_content: str, llm: Any
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
- Explicit API key labels ("Your API key:", "API Key:", "Access Key:", "Token:", "Key:")
- Long alphanumeric strings (typically 20-64 characters)
- Keys in code blocks or highlighted text
- UUID format keys (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)
- Base64 encoded strings

Return ONLY the API key string if found, or "NOT_FOUND" if no key is present.
Do not include any explanation, labels, or formatting - just the key or NOT_FOUND.
"""

        try:
            response = llm.invoke([HumanMessage(content=prompt)])
            extracted = response.content.strip()

            # Remove common artifacts
            extracted = extracted.replace("`", "").replace('"', "").replace("'", "")

            if extracted and extracted != "NOT_FOUND" and len(extracted) > 10:
                logger.info(f"Extracted API key from email (length: {len(extracted)})")
                return extracted

            logger.warning("No API key found in email")
            return None

        except Exception as e:
            logger.error(f"Failed to extract API key: {e}")
            return None

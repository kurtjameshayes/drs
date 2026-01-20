"""
API Key Store Service

Manages encrypted storage and retrieval of API keys in MongoDB.
"""

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
        encryption_key = Config.API_KEY_ENCRYPTION_KEY
        if not encryption_key:
            logger.warning(
                "API_KEY_ENCRYPTION_KEY not set, generating new key. "
                "Note: Keys will not be decryptable after restart."
            )
            encryption_key = Fernet.generate_key().decode()

        self.cipher = Fernet(
            encryption_key.encode() if isinstance(encryption_key, str) else encryption_key
        )

        # Create indexes
        self.collection.create_index("source_identifier", unique=True)
        self.collection.create_index("status")
        logger.info("API Key Store initialized")

    def _normalize_identifier(self, identifier: str) -> str:
        """
        Normalize source identifier (URL or name) to consistent format.

        Args:
            identifier: URL or source name

        Returns:
            Normalized identifier (lowercase domain or name)
        """
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

        record = self.collection.find_one(
            {"source_identifier": identifier, "status": "active"}
        )

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
        self, source_identifier: str, api_key: str, metadata: Dict[str, Any]
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
                "metadata": metadata,
            }

            self.collection.update_one(
                {"source_identifier": identifier}, {"$set": record}, upsert=True
            )

            logger.info(f"Stored API key for {identifier}")
            return True

        except Exception as e:
            logger.error(f"Failed to store API key for {identifier}: {e}")
            return False

    def key_exists(self, source_identifier: str) -> bool:
        """
        Check if we have an active key for this source.

        Args:
            source_identifier: Data source URL or normalized name

        Returns:
            True if an active key exists
        """
        identifier = self._normalize_identifier(source_identifier)

        count = self.collection.count_documents(
            {"source_identifier": identifier, "status": "active"}
        )

        return count > 0

    def invalidate_key(self, source_identifier: str) -> bool:
        """
        Mark a key as invalid (e.g., after auth failure).

        Args:
            source_identifier: Data source URL or normalized name

        Returns:
            True if key was invalidated
        """
        identifier = self._normalize_identifier(source_identifier)

        result = self.collection.update_one(
            {"source_identifier": identifier},
            {"$set": {"status": "invalid", "invalidated_at": datetime.utcnow()}},
        )

        if result.modified_count > 0:
            logger.info(f"Invalidated API key for {identifier}")
            return True

        return False

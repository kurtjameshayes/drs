#!/usr/bin/env python3
"""
Generate a Fernet encryption key for API key storage.

This script generates a cryptographically secure encryption key that can be
used for the API_KEY_ENCRYPTION_KEY environment variable.

Usage:
    python3 scripts/generate_encryption_key.py

The generated key should be added to your .env file:
    API_KEY_ENCRYPTION_KEY=<generated_key>

WARNING: Once you start using an encryption key, do not change it!
         Changing the key will make all previously encrypted API keys unreadable.
"""

import os
import base64

def generate_key():
    """Generate a new Fernet encryption key."""
    key = base64.urlsafe_b64encode(os.urandom(32)).decode()
    return key

if __name__ == "__main__":
    key = generate_key()
    print("=" * 80)
    print("Generated Encryption Key")
    print("=" * 80)
    print()
    print(f"API_KEY_ENCRYPTION_KEY={key}")
    print()
    print("=" * 80)
    print("IMPORTANT:")
    print("  1. Copy the key above and add it to your .env file")
    print("  2. Do NOT share this key or commit it to version control")
    print("  3. Do NOT change this key once you've started using it")
    print("  4. Store a backup of this key in a secure location")
    print("=" * 80)

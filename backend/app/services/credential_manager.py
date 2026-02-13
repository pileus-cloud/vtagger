"""
VTagger Credential Manager.

Manages secure storage and retrieval of API credentials.
Storage backends: environment variables or encrypted config file.

Encryption: Fernet symmetric encryption (AES-128-CBC) with master key.
"""

import base64
import hashlib
import json
import os
import platform
import getpass
from pathlib import Path
from typing import Optional, Tuple

from cryptography.fernet import Fernet

SERVICE_NAME = "vtagger"
CONFIG_DIR = Path.home() / ".vtagger"
CONFIG_FILE = CONFIG_DIR / "credentials.json"


def _derive_key(master_key: str) -> bytes:
    """Derive a Fernet-compatible key from master key using SHA-256."""
    digest = hashlib.sha256(master_key.encode()).digest()
    return base64.urlsafe_b64encode(digest)


def _get_machine_key() -> str:
    """Generate a machine-specific fallback key from hostname, username, and service."""
    hostname = platform.node()
    username = getpass.getuser()
    salt = f"{hostname}:{username}:{SERVICE_NAME}"
    return hashlib.sha256(salt.encode()).hexdigest()


def _get_master_key() -> str:
    """Get master key from hierarchy: env var > config file > machine-derived fallback."""
    # 1. Environment variable
    env_key = os.environ.get("VTAGGER_MASTER_KEY")
    if env_key:
        return env_key

    # 2. Config file
    if CONFIG_FILE.exists():
        try:
            config = json.loads(CONFIG_FILE.read_text())
            if config.get("master_key"):
                return config["master_key"]
        except (json.JSONDecodeError, IOError):
            pass

    # 3. Machine-derived fallback
    return _get_machine_key()


def _encrypt_value(value: str, master_key: str) -> str:
    """Encrypt a value using Fernet."""
    key = _derive_key(master_key)
    f = Fernet(key)
    return f.encrypt(value.encode()).decode("ascii")


def _decrypt_value(encrypted: str, master_key: str) -> str:
    """Decrypt a value using Fernet."""
    key = _derive_key(master_key)
    f = Fernet(key)
    return f.decrypt(encrypted.encode()).decode("utf-8")


def set_credentials(username: str, password: str) -> bool:
    """Store credentials in encrypted config file (~/.vtagger/credentials.json)."""
    try:
        master_key = _get_master_key()
        encrypted_username = _encrypt_value(username, master_key)
        encrypted_password = _encrypt_value(password, master_key)

        CONFIG_DIR.mkdir(parents=True, exist_ok=True)

        config = {}
        if CONFIG_FILE.exists():
            try:
                config = json.loads(CONFIG_FILE.read_text())
            except (json.JSONDecodeError, IOError):
                config = {}

        config["username"] = encrypted_username
        config["password"] = encrypted_password
        config["encrypted"] = True

        CONFIG_FILE.write_text(json.dumps(config, indent=2))
        CONFIG_FILE.chmod(0o600)

        return True

    except Exception as e:
        print(f"Error storing credentials: {e}")
        return False


def get_credentials() -> Optional[Tuple[str, str]]:
    """Retrieve stored credentials.

    Retrieval hierarchy:
    1. Environment variables (VTAGGER_USERNAME, VTAGGER_PASSWORD)
    2. Encrypted config file (~/.vtagger/credentials.json)

    Returns:
        Tuple of (username, password) or None if not found.
    """
    # 1. Environment variables
    env_user = os.environ.get("VTAGGER_USERNAME")
    env_pass = os.environ.get("VTAGGER_PASSWORD")
    if env_user and env_pass:
        return (env_user, env_pass)

    # 2. Encrypted config file
    if CONFIG_FILE.exists():
        try:
            config = json.loads(CONFIG_FILE.read_text())
            master_key = _get_master_key()

            if config.get("encrypted"):
                username = _decrypt_value(config["username"], master_key)
                password = _decrypt_value(config["password"], master_key)
            else:
                username = config.get("username", "")
                password = config.get("password", "")

            if username and password:
                return (username, password)
        except Exception as e:
            print(f"Error reading credentials: {e}")

    return None


def has_credentials() -> bool:
    """Check if credentials are configured (without decrypting)."""
    if os.environ.get("VTAGGER_USERNAME") and os.environ.get("VTAGGER_PASSWORD"):
        return True

    if CONFIG_FILE.exists():
        try:
            config = json.loads(CONFIG_FILE.read_text())
            if config.get("username") and config.get("password"):
                return True
        except (json.JSONDecodeError, IOError):
            pass

    return False


def verify_credentials() -> Tuple[bool, str]:
    """Verify that credentials can be retrieved and decrypted.

    Returns:
        Tuple of (success: bool, message: str)
    """
    if not has_credentials():
        return (False, "No credentials found. Run 'vtagger credentials set' to configure.")

    creds = get_credentials()
    if creds is None:
        return (False, "Credentials found but could not be decrypted. "
                       "Run 'vtagger credentials set' to reconfigure.")

    username, password = creds
    if not username or not password:
        return (False, "Credentials are empty. Run 'vtagger credentials set' to configure.")

    masked = password[:2] + "*" * (len(password) - 4) + password[-2:] if len(password) > 4 else "****"
    return (True, f"Credentials OK (user: {username}, pass: {masked})")


def delete_credentials() -> bool:
    """Delete stored credentials."""
    if CONFIG_FILE.exists():
        try:
            CONFIG_FILE.unlink()
            return True
        except OSError:
            pass

    return False

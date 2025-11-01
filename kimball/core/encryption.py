"""
KIMBALL Encryption Utilities

This module provides encryption and decryption functionality for sensitive
data stored in metadata tables, particularly for connection credentials in metadata.acquire.
"""

import base64
import os
from typing import Optional
from cryptography.fernet import Fernet

from .logger import Logger


class EncryptionManager:
    """Manages encryption and decryption of sensitive data."""
    
    def __init__(self, encryption_key: Optional[str] = None):
        """
        Initialize the encryption manager.
        
        Args:
            encryption_key: Optional encryption key. If not provided, uses environment
                          variable KIMBALL_ENCRYPTION_KEY or generates a default key.
                          The key should be a base64-encoded Fernet key.
        """
        self.logger = Logger("encryption")
        self.key = self._get_or_create_key(encryption_key)
        self.cipher = Fernet(self.key)
    
    def _get_or_create_key(self, provided_key: Optional[str]) -> bytes:
        """Get encryption key from various sources or generate a default."""
        if provided_key:
            try:
                # Use provided key if it's base64-encoded
                return base64.urlsafe_b64decode(provided_key)
            except Exception as e:
                self.logger.warning(f"Invalid provided key format, generating new key: {e}")
        
        # Try to get from environment variable
        env_key = os.getenv('KIMBALL_ENCRYPTION_KEY')
        if env_key:
            try:
                return base64.urlsafe_b64decode(env_key)
            except Exception as e:
                self.logger.warning(f"Invalid environment key format, generating new key: {e}")
        
        # Generate a default key (NOTE: In production, this should be set securely)
        # For default key, we generate a Fernet key directly
        # In production, this should be set via KIMBALL_ENCRYPTION_KEY environment variable
        key = Fernet.generate_key()
        
        self.logger.warning(
            "Using generated encryption key. For production, set KIMBALL_ENCRYPTION_KEY "
            "environment variable with a secure base64-encoded Fernet key. "
            "Current key will be different on each restart."
        )
        
        return key
    
    @staticmethod
    def generate_key() -> str:
        """
        Generate a new encryption key.
        
        Returns:
            str: Base64-encoded Fernet key suitable for KIMBALL_ENCRYPTION_KEY
        """
        key = Fernet.generate_key()
        return base64.urlsafe_b64encode(key).decode()
    
    def encrypt(self, plaintext: str) -> str:
        """
        Encrypt a string value.
        
        Args:
            plaintext: The string to encrypt
            
        Returns:
            str: Encrypted string (base64-encoded)
        """
        if not plaintext:
            return ""
        
        try:
            encrypted_bytes = self.cipher.encrypt(plaintext.encode('utf-8'))
            return base64.urlsafe_b64encode(encrypted_bytes).decode('utf-8')
        except Exception as e:
            self.logger.error(f"Error encrypting data: {e}")
            raise ValueError(f"Encryption failed: {e}")
    
    def decrypt(self, ciphertext: str) -> str:
        """
        Decrypt a string value.
        
        Args:
            ciphertext: The encrypted string (base64-encoded)
            
        Returns:
            str: Decrypted plaintext string
        """
        if not ciphertext:
            return ""
        
        try:
            decoded_bytes = base64.urlsafe_b64decode(ciphertext.encode('utf-8'))
            decrypted_bytes = self.cipher.decrypt(decoded_bytes)
            return decrypted_bytes.decode('utf-8')
        except Exception as e:
            self.logger.error(f"Error decrypting data: {e}")
            raise ValueError(f"Decryption failed: {e}")
    
    def encrypt_connection_config(self, config: dict) -> dict:
        """
        Encrypt sensitive fields in a connection configuration dictionary.
        
        Encrypts: password, secret_key, access_key, api_token, secret
        
        Args:
            config: Connection configuration dictionary
            
        Returns:
            dict: Configuration with sensitive fields encrypted
        """
        sensitive_fields = ['password', 'secret_key', 'access_key', 'api_token', 'secret']
        encrypted_config = config.copy()
        
        for field in sensitive_fields:
            if field in encrypted_config and encrypted_config[field]:
                encrypted_config[field] = self.encrypt(str(encrypted_config[field]))
        
        return encrypted_config
    
    def decrypt_connection_config(self, config: dict) -> dict:
        """
        Decrypt sensitive fields in a connection configuration dictionary.
        
        Decrypts: password, secret_key, access_key, api_token, secret
        
        Args:
            config: Connection configuration dictionary with encrypted fields
            
        Returns:
            dict: Configuration with sensitive fields decrypted
        """
        sensitive_fields = ['password', 'secret_key', 'access_key', 'api_token', 'secret']
        decrypted_config = config.copy()
        
        for field in sensitive_fields:
            if field in decrypted_config and decrypted_config[field]:
                try:
                    decrypted_config[field] = self.decrypt(str(decrypted_config[field]))
                except Exception as e:
                    self.logger.warning(f"Failed to decrypt field {field}: {e}")
                    # Keep encrypted value if decryption fails
                    pass
        
        return decrypted_config


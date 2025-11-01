"""
KIMBALL Encryption Utilities

This module provides encryption and decryption functionality for sensitive
data stored in metadata tables, particularly for connection credentials in metadata.acquire.
"""

import base64
import os
import json
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
                          variable KIMBALL_ENCRYPTION_KEY, then config.json, or generates
                          a persistent key stored in config.json.
                          The key should be a base64-encoded Fernet key.
        """
        self.logger = Logger("encryption")
        self.key = self._get_or_create_key(encryption_key)
        self.cipher = Fernet(self.key)
    
    def _get_or_create_key(self, provided_key: Optional[str]) -> bytes:
        """
        Get encryption key from various sources in priority order:
        1. Provided key (if passed explicitly)
        2. Environment variable KIMBALL_ENCRYPTION_KEY (production use)
        3. config.json encryption_key (persistent across restarts)
        4. Generate new key and store in config.json
        """
        # Priority 1: Use provided key if explicitly passed
        if provided_key:
            try:
                return base64.urlsafe_b64decode(provided_key)
            except Exception as e:
                self.logger.warning(f"Invalid provided key format, trying other sources: {e}")
        
        # Priority 2: Try environment variable (highest priority for production)
        env_key = os.getenv('KIMBALL_ENCRYPTION_KEY')
        if env_key:
            try:
                key_bytes = base64.urlsafe_b64decode(env_key)
                self.logger.info("Using encryption key from KIMBALL_ENCRYPTION_KEY environment variable")
                return key_bytes
            except Exception as e:
                self.logger.warning(f"Invalid environment key format, trying config.json: {e}")
        
        # Priority 3: Try to get from config.json
        try:
            config_file = os.getenv('KIMBALL_CONFIG_FILE', 'config.json')
            if os.path.exists(config_file):
                with open(config_file, 'r') as f:
                    config = json.load(f)
                    if 'encryption_key' in config:
                        try:
                            key_bytes = base64.urlsafe_b64decode(config['encryption_key'])
                            self.logger.info("Using encryption key from config.json")
                            return key_bytes
                        except Exception as e:
                            self.logger.warning(f"Invalid encryption key in config.json, generating new: {e}")
        except Exception as e:
            self.logger.warning(f"Error reading config.json for encryption key: {e}")
        
        # Priority 4: Generate new key and store in config.json for persistence
        key = Fernet.generate_key()
        key_base64 = base64.urlsafe_b64encode(key).decode()
        
        try:
            # Store the key in config.json for persistence
            config_file = os.getenv('KIMBALL_CONFIG_FILE', 'config.json')
            config = {}
            if os.path.exists(config_file):
                with open(config_file, 'r') as f:
                    config = json.load(f)
            
            config['encryption_key'] = key_base64
            
            # Write back to config.json
            with open(config_file, 'w') as f:
                json.dump(config, f, indent=2)
            
            self.logger.info(
                f"Generated and stored persistent encryption key in {config_file}. "
                "For production, consider using KIMBALL_ENCRYPTION_KEY environment variable."
            )
        except Exception as e:
            self.logger.warning(
                f"Could not store encryption key in config.json: {e}. "
                "Key will be different on each restart unless KIMBALL_ENCRYPTION_KEY is set."
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


"""
Configuration Manager for ESL Middleware
Handles loading and saving configuration from JSON files
"""

import os
import json
from typing import Optional, Dict, Any
from pathlib import Path


class Config:
    """Configuration settings for the middleware"""
    
    def __init__(self, config_file: str = "config.json"):
        """
        Initialize configuration with defaults and load from file if exists
        
        Args:
            config_file: Path to the JSON configuration file
        """
        self.config_file: str = config_file
        
        # Initialize with default values (mutable attributes)
        self.DBF_INPUT_DIR: str = "./RMan_Export/"
        self.CSV_OUTPUT_DIR: str = "./ESL_Sync/"
        self.LOG_DIR: str = "./ESL_Middleware_Logs/"
        self.STATE_FILE: str = "state.json"
        self.POLL_INTERVAL: int = 30
        self.MAX_RETRIES: int = 3
        self.RETRY_DELAY: int = 2
        
        # Additional configuration options
        self.BATCH_SIZE: int = 1000
        self.FILE_LOCK_TIMEOUT: int = 10
        self.FILE_LOCK_RETRY_DELAY: float = 0.5
        self.CSV_ENCODING: str = "utf-8"
        self.CSV_DELIMITER: str = ","
        self.PRESERVE_BACKUP_COUNT: int = 5
        self.MONITOR_FILE_PATTERNS: list = ["*.DBF", "*.dbf"]
        self.EXCLUDED_FIELDS: list = ["TIMESTAMP", "MODIFIED", "DELETED"]
        self.DEBUG_MODE: bool = False
        
        # Load from file if it exists
        self.load_from_file()
    
    def load_defaults(self) -> None:
        """Set default configuration values"""
        # Platform-specific defaults
        if os.name == 'nt':  # Windows
            self.DBF_INPUT_DIR = "C:\\RMan_Export"
            self.CSV_OUTPUT_DIR = "C:\\ESL_Sync"
            self.LOG_DIR = "C:\\ESL_Middleware_Logs"
        else:  # Unix/Mac
            self.DBF_INPUT_DIR = "./RMan_Export/"
            self.CSV_OUTPUT_DIR = "./ESL_Sync/"
            self.LOG_DIR = "./ESL_Middleware_Logs/"
    
    def load_from_file(self) -> bool:
        """
        Load configuration from JSON file if it exists
        
        Returns:
            True if config was loaded, False otherwise
        """
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r') as f:
                    config_data = json.load(f)
                
                # Update attributes from loaded data
                for key, value in config_data.items():
                    if hasattr(self, key):
                        setattr(self, key, value)
                
                return True
                
            except (json.JSONDecodeError, IOError) as e:
                print(f"Warning: Could not load config file {self.config_file}: {e}")
                return False
        return False
    
    def save_to_file(self, file_path: Optional[str] = None) -> bool:
        """
        Save current configuration to JSON file
        
        Args:
            file_path: Optional path to save to (uses self.config_file if not provided)
            
        Returns:
            True if save was successful, False otherwise
        """
        save_path = file_path or self.config_file
        
        # Build configuration dictionary
        config_data = {
            "DBF_INPUT_DIR": self.DBF_INPUT_DIR,
            "CSV_OUTPUT_DIR": self.CSV_OUTPUT_DIR,
            "LOG_DIR": self.LOG_DIR,
            "STATE_FILE": self.STATE_FILE,
            "POLL_INTERVAL": self.POLL_INTERVAL,
            "MAX_RETRIES": self.MAX_RETRIES,
            "RETRY_DELAY": self.RETRY_DELAY,
            "BATCH_SIZE": self.BATCH_SIZE,
            "FILE_LOCK_TIMEOUT": self.FILE_LOCK_TIMEOUT,
            "FILE_LOCK_RETRY_DELAY": self.FILE_LOCK_RETRY_DELAY,
            "CSV_ENCODING": self.CSV_ENCODING,
            "CSV_DELIMITER": self.CSV_DELIMITER,
            "PRESERVE_BACKUP_COUNT": self.PRESERVE_BACKUP_COUNT,
            "MONITOR_FILE_PATTERNS": self.MONITOR_FILE_PATTERNS,
            "EXCLUDED_FIELDS": self.EXCLUDED_FIELDS,
            "DEBUG_MODE": self.DEBUG_MODE
        }
        
        try:
            with open(save_path, 'w') as f:
                json.dump(config_data, f, indent=4)
            return True
            
        except IOError as e:
            print(f"Error: Could not save config file {save_path}: {e}")
            return False
    
    def update(self, updates: Dict[str, Any]) -> None:
        """
        Update multiple configuration values at once
        
        Args:
            updates: Dictionary of configuration updates
        """
        for key, value in updates.items():
            if hasattr(self, key):
                setattr(self, key, value)
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert configuration to dictionary
        
        Returns:
            Dictionary of configuration values
        """
        return {
            "DBF_INPUT_DIR": self.DBF_INPUT_DIR,
            "CSV_OUTPUT_DIR": self.CSV_OUTPUT_DIR,
            "LOG_DIR": self.LOG_DIR,
            "STATE_FILE": self.STATE_FILE,
            "POLL_INTERVAL": self.POLL_INTERVAL,
            "MAX_RETRIES": self.MAX_RETRIES,
            "RETRY_DELAY": self.RETRY_DELAY,
            "BATCH_SIZE": self.BATCH_SIZE,
            "FILE_LOCK_TIMEOUT": self.FILE_LOCK_TIMEOUT,
            "FILE_LOCK_RETRY_DELAY": self.FILE_LOCK_RETRY_DELAY,
            "CSV_ENCODING": self.CSV_ENCODING,
            "CSV_DELIMITER": self.CSV_DELIMITER,
            "PRESERVE_BACKUP_COUNT": self.PRESERVE_BACKUP_COUNT,
            "MONITOR_FILE_PATTERNS": self.MONITOR_FILE_PATTERNS,
            "EXCLUDED_FIELDS": self.EXCLUDED_FIELDS,
            "DEBUG_MODE": self.DEBUG_MODE
        }
    
    def validate(self) -> tuple[bool, list[str]]:
        """
        Validate configuration values
        
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        # Check required directories exist or can be created
        for dir_attr in ['DBF_INPUT_DIR', 'CSV_OUTPUT_DIR', 'LOG_DIR']:
            dir_path = getattr(self, dir_attr)
            if not dir_path:
                errors.append(f"{dir_attr} is not set")
        
        # Validate numeric ranges
        if self.POLL_INTERVAL < 1:
            errors.append("POLL_INTERVAL must be at least 1 second")
        
        if self.MAX_RETRIES < 1:
            errors.append("MAX_RETRIES must be at least 1")
        
        if self.RETRY_DELAY < 0:
            errors.append("RETRY_DELAY cannot be negative")
        
        return (len(errors) == 0, errors)
    
    def ensure_directories(self) -> bool:
        """
        Create all configured directories if they don't exist
        
        Returns:
            True if all directories exist or were created, False otherwise
        """
        success = True
        
        for dir_attr in ['DBF_INPUT_DIR', 'CSV_OUTPUT_DIR', 'LOG_DIR']:
            dir_path = getattr(self, dir_attr)
            try:
                Path(dir_path).mkdir(parents=True, exist_ok=True)
            except Exception as e:
                print(f"Error creating directory {dir_path}: {e}")
                success = False
        
        return success
    
    def __repr__(self) -> str:
        """String representation of configuration"""
        return f"Config(file='{self.config_file}', poll={self.POLL_INTERVAL}s)"
    
    def __str__(self) -> str:
        """Human-readable string representation"""
        return (
            f"ESL Middleware Configuration:\n"
            f"  Input: {self.DBF_INPUT_DIR}\n"
            f"  Output: {self.CSV_OUTPUT_DIR}\n"
            f"  Logs: {self.LOG_DIR}\n"
            f"  Poll Interval: {self.POLL_INTERVAL} seconds\n"
            f"  Debug Mode: {self.DEBUG_MODE}"
        )


def create_default_config(file_path: str = "config.json") -> Config:
    """
    Create and save a default configuration file
    
    Args:
        file_path: Path where to save the config file
        
    Returns:
        Config object with default settings
    """
    config = Config(file_path)
    config.save_to_file()
    print(f"Default configuration created: {file_path}")
    return config


def load_or_create_config(file_path: str = "config.json") -> Config:
    """
    Load existing config or create new one with defaults
    
    Args:
        file_path: Path to the config file
        
    Returns:
        Config object
    """
    config = Config(file_path)
    
    if not os.path.exists(file_path):
        config.save_to_file()
        print(f"Created new configuration file: {file_path}")
    else:
        print(f"Loaded configuration from: {file_path}")
    
    return config


if __name__ == "__main__":
    # Example usage
    print("Configuration Manager Demo")
    print("-" * 40)
    
    # Load or create config
    config = load_or_create_config()
    
    # Display current configuration
    print("\nCurrent Configuration:")
    print(config)
    
    # Validate configuration
    is_valid, errors = config.validate()
    if is_valid:
        print("\n✅ Configuration is valid")
    else:
        print("\n❌ Configuration errors:")
        for error in errors:
            print(f"  - {error}")
    
    # Ensure directories exist
    if config.ensure_directories():
        print("\n✅ All directories verified/created")
    
    # Example: Update configuration
    print("\nUpdating configuration...")
    config.POLL_INTERVAL = 60
    config.DEBUG_MODE = True
    config.save_to_file()
    print("Configuration saved with updates")
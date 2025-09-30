#!/usr/bin/env python3
"""
Fixed Test Suite for ESL Middleware
Compatible with the updated Config class
"""

import unittest
import os
import sys
import json
import tempfile
import shutil
from pathlib import Path
from datetime import datetime
from decimal import Decimal

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import the fixed Config class
from config_manager import Config, create_default_config, load_or_create_config


class TestConfig(unittest.TestCase):
    """Test configuration management with fixed Config class"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.config_file = os.path.join(self.temp_dir, "test_config.json")
        
    def tearDown(self):
        """Clean up test fixtures"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        
    def test_default_config(self):
        """Test loading default configuration"""
        config = Config(self.config_file)
        
        # Check default values
        self.assertEqual(config.POLL_INTERVAL, 30)
        self.assertEqual(config.MAX_RETRIES, 3)
        self.assertEqual(config.RETRY_DELAY, 2)
        self.assertEqual(config.BATCH_SIZE, 1000)
        
    def test_modify_config_attributes(self):
        """Test that we can modify configuration attributes"""
        config = Config(self.config_file)
        
        # These should work now without type errors
        config.POLL_INTERVAL = 45
        config.CSV_OUTPUT_DIR = "/custom/output"
        config.DEBUG_MODE = True
        
        self.assertEqual(config.POLL_INTERVAL, 45)
        self.assertEqual(config.CSV_OUTPUT_DIR, "/custom/output")
        self.assertTrue(config.DEBUG_MODE)
        
    def test_load_from_file(self):
        """Test loading configuration from file"""
        test_config = {
            "DBF_INPUT_DIR": "/test/input",
            "CSV_OUTPUT_DIR": "/test/output",
            "POLL_INTERVAL": 60,
            "DEBUG_MODE": True
        }
        
        with open(self.config_file, 'w') as f:
            json.dump(test_config, f)
            
        config = Config(self.config_file)
        
        self.assertEqual(config.DBF_INPUT_DIR, "/test/input")
        self.assertEqual(config.CSV_OUTPUT_DIR, "/test/output")
        self.assertEqual(config.POLL_INTERVAL, 60)
        self.assertTrue(config.DEBUG_MODE)
        
    def test_save_to_file(self):
        """Test saving configuration to file"""
        config = Config(self.config_file)
        
        # Modify configuration
        config.POLL_INTERVAL = 45
        config.CSV_OUTPUT_DIR = "/modified/output"
        config.DEBUG_MODE = True
        
        # Save to file
        success = config.save_to_file()
        self.assertTrue(success)
        
        # Load and verify
        with open(self.config_file, 'r') as f:
            saved_config = json.load(f)
            
        self.assertEqual(saved_config["POLL_INTERVAL"], 45)
        self.assertEqual(saved_config["CSV_OUTPUT_DIR"], "/modified/output")
        self.assertTrue(saved_config["DEBUG_MODE"])
        
    def test_update_method(self):
        """Test bulk update of configuration"""
        config = Config(self.config_file)
        
        updates = {
            "POLL_INTERVAL": 90,
            "MAX_RETRIES": 5,
            "DEBUG_MODE": True,
            "BATCH_SIZE": 2000
        }
        
        config.update(updates)
        
        self.assertEqual(config.POLL_INTERVAL, 90)
        self.assertEqual(config.MAX_RETRIES, 5)
        self.assertTrue(config.DEBUG_MODE)
        self.assertEqual(config.BATCH_SIZE, 2000)
        
    def test_validate_configuration(self):
        """Test configuration validation"""
        config = Config(self.config_file)
        
        # Valid configuration
        is_valid, errors = config.validate()
        self.assertTrue(is_valid)
        self.assertEqual(len(errors), 0)
        
        # Invalid configuration
        config.POLL_INTERVAL = 0  # Invalid: must be at least 1
        config.MAX_RETRIES = 0    # Invalid: must be at least 1
        
        is_valid, errors = config.validate()
        self.assertFalse(is_valid)
        self.assertGreater(len(errors), 0)
        
    def test_to_dict(self):
        """Test conversion to dictionary"""
        config = Config(self.config_file)
        config.POLL_INTERVAL = 45
        
        config_dict = config.to_dict()
        
        self.assertIsInstance(config_dict, dict)
        self.assertEqual(config_dict["POLL_INTERVAL"], 45)
        self.assertIn("CSV_OUTPUT_DIR", config_dict)
        self.assertIn("DBF_INPUT_DIR", config_dict)
        
    def test_ensure_directories(self):
        """Test directory creation"""
        config = Config(self.config_file)
        
        # Set test directories
        config.DBF_INPUT_DIR = os.path.join(self.temp_dir, "input")
        config.CSV_OUTPUT_DIR = os.path.join(self.temp_dir, "output")
        config.LOG_DIR = os.path.join(self.temp_dir, "logs")
        
        # Ensure directories are created
        success = config.ensure_directories()
        self.assertTrue(success)
        
        # Verify directories exist
        self.assertTrue(os.path.exists(config.DBF_INPUT_DIR))
        self.assertTrue(os.path.exists(config.CSV_OUTPUT_DIR))
        self.assertTrue(os.path.exists(config.LOG_DIR))
        
    def test_create_default_config(self):
        """Test creating default configuration file"""
        config_path = os.path.join(self.temp_dir, "default_config.json")
        
        config = create_default_config(config_path)
        
        self.assertTrue(os.path.exists(config_path))
        self.assertIsInstance(config, Config)
        self.assertEqual(config.POLL_INTERVAL, 30)
        
    def test_load_or_create_config(self):
        """Test load or create configuration logic"""
        config_path = os.path.join(self.temp_dir, "auto_config.json")
        
        # First call should create config
        config1 = load_or_create_config(config_path)
        self.assertTrue(os.path.exists(config_path))
        
        # Modify and save
        config1.POLL_INTERVAL = 60
        config1.save_to_file()
        
        # Second call should load existing config
        config2 = load_or_create_config(config_path)
        self.assertEqual(config2.POLL_INTERVAL, 60)


class TestConfigIntegration(unittest.TestCase):
    """Integration tests for configuration in middleware context"""
    
    def setUp(self):
        """Set up test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.config_file = os.path.join(self.temp_dir, "integration_config.json")
        
    def tearDown(self):
        """Clean up test environment"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        
    def test_config_with_middleware_components(self):
        """Test configuration usage with middleware components"""
        # Create configuration
        config = Config(self.config_file)
        
        # Set middleware-specific paths
        config.DBF_INPUT_DIR = os.path.join(self.temp_dir, "dbf_files")
        config.CSV_OUTPUT_DIR = os.path.join(self.temp_dir, "csv_output")
        config.LOG_DIR = os.path.join(self.temp_dir, "logs")
        config.STATE_FILE = os.path.join(self.temp_dir, "state.json")
        
        # Ensure directories
        config.ensure_directories()
        
        # Verify all paths are accessible
        self.assertTrue(os.path.exists(config.DBF_INPUT_DIR))
        self.assertTrue(os.path.exists(config.CSV_OUTPUT_DIR))
        self.assertTrue(os.path.exists(config.LOG_DIR))
        
        # Save configuration
        config.save_to_file()
        self.assertTrue(os.path.exists(self.config_file))
        
    def test_platform_specific_defaults(self):
        """Test platform-specific default paths"""
        config = Config(self.config_file)
        
        # The defaults should be set based on the platform
        if os.name == 'nt':  # Windows
            self.assertIn("C:\\", config.DBF_INPUT_DIR)
        else:  # Unix/Mac
            self.assertIn("./", config.DBF_INPUT_DIR)


class TestSimpleComponents(unittest.TestCase):
    """Simple tests for basic components that don't require full imports"""
    
    def test_decimal_handling(self):
        """Test decimal price handling"""
        price = Decimal('29.99')
        self.assertEqual(str(price), '29.99')
        self.assertEqual(price * 2, Decimal('59.98'))
        
    def test_timestamp_generation(self):
        """Test UTC timestamp generation"""
        timestamp = datetime.utcnow().isoformat() + 'Z'
        self.assertTrue(timestamp.endswith('Z'))
        self.assertIn('T', timestamp)
        
    def test_csv_filename_pattern(self):
        """Test CSV filename generation pattern"""
        source_file = "STOCK.DBF"
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        filename = f"{Path(source_file).stem}_{timestamp}.csv"
        
        self.assertTrue(filename.startswith("STOCK_"))
        self.assertTrue(filename.endswith(".csv"))
        self.assertEqual(len(timestamp), 14)


def run_tests():
    """Run all tests with proper test discovery"""
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add test classes
    suite.addTests(loader.loadTestsFromTestCase(TestConfig))
    suite.addTests(loader.loadTestsFromTestCase(TestConfigIntegration))
    suite.addTests(loader.loadTestsFromTestCase(TestSimpleComponents))
    
    # Run tests with verbosity
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    print("\n" + "=" * 70)
    print("TEST SUMMARY")
    print("=" * 70)
    print(f"Tests Run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Success Rate: {((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100):.1f}%")
    
    # Return success/failure
    return result.wasSuccessful()


if __name__ == "__main__":
    print("ESL Middleware Test Suite")
    print("=" * 70)
    print("Testing Configuration Manager and Basic Components")
    print("-" * 70 + "\n")
    
    success = run_tests()
    
    if success:
        print("\n✅ All tests passed!")
    else:
        print("\n❌ Some tests failed. Check the output above for details.")
    
    sys.exit(0 if success else 1)
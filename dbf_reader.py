"""
ESL Inventory Synchronization Middleware - Step 1: DBF Reader
Reads .dbf files from R-MPOS export directory
"""

import os
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
import json

# Third-party imports
try:
    from dbfread import DBF
    from loguru import logger
    import pandas as pd
except ImportError as e:
    print(f"Missing required library: {e}")
    print("Please install: pip install dbfread loguru pandas")
    sys.exit(1)

# Configuration
class Config:
    """Configuration settings for the middleware"""
    def __init__(self, config_file: str = "config.json"):
        self.config_file = config_file
        self.load_defaults()
        self.load_from_file()
    
    def load_defaults(self):
        """Set default configuration values"""
        self.DBF_INPUT_DIR = r"C:\RMan_Export"
        self.CSV_OUTPUT_DIR = r"C:\ESL_Sync"
        self.LOG_DIR = r"C:\ESL_Middleware_Logs"
        self.STATE_FILE = "state.json"
        self.POLL_INTERVAL = 30  # seconds
        self.MAX_RETRIES = 3
        self.RETRY_DELAY = 2  # seconds
        
    def load_from_file(self):
        """Load configuration from JSON file if it exists"""
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r') as f:
                    config_data = json.load(f)
                    for key, value in config_data.items():
                        setattr(self, key, value)
                logger.info(f"Configuration loaded from {self.config_file}")
            except Exception as e:
                logger.warning(f"Could not load config file: {e}. Using defaults.")
    
    def save_to_file(self):
        """Save current configuration to JSON file"""
        config_data = {
            "DBF_INPUT_DIR": self.DBF_INPUT_DIR,
            "CSV_OUTPUT_DIR": self.CSV_OUTPUT_DIR,
            "LOG_DIR": self.LOG_DIR,
            "STATE_FILE": self.STATE_FILE,
            "POLL_INTERVAL": self.POLL_INTERVAL,
            "MAX_RETRIES": self.MAX_RETRIES,
            "RETRY_DELAY": self.RETRY_DELAY
        }
        try:
            with open(self.config_file, 'w') as f:
                json.dump(config_data, f, indent=4)
            logger.info(f"Configuration saved to {self.config_file}")
        except Exception as e:
            logger.error(f"Could not save config file: {e}")


class DBFReader:
    """Handles reading and processing of .dbf files"""
    
    def __init__(self, config: Config):
        self.config = config
        self.setup_logging()
        self.ensure_directories()
        
    def setup_logging(self):
        """Configure logging with loguru"""
        log_file = Path(self.config.LOG_DIR) / f"esl_sync_{datetime.now():%Y%m%d}.log"
        logger.add(
            log_file,
            rotation="1 day",
            retention="30 days",
            level="DEBUG",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
        )
        logger.info("=" * 50)
        logger.info("ESL Synchronization Middleware Started")
        logger.info(f"DBF Input Directory: {self.config.DBF_INPUT_DIR}")
        logger.info(f"CSV Output Directory: {self.config.CSV_OUTPUT_DIR}")
        
    def ensure_directories(self):
        """Create necessary directories if they don't exist"""
        for dir_path in [self.config.DBF_INPUT_DIR, 
                         self.config.CSV_OUTPUT_DIR, 
                         self.config.LOG_DIR]:
            Path(dir_path).mkdir(parents=True, exist_ok=True)
            logger.debug(f"Ensured directory exists: {dir_path}")
    
    def find_dbf_files(self) -> List[Path]:
        """Find all .dbf files in the input directory"""
        dbf_dir = Path(self.config.DBF_INPUT_DIR)
        dbf_files = list(dbf_dir.glob("*.dbf")) + list(dbf_dir.glob("*.DBF"))
        logger.info(f"Found {len(dbf_files)} DBF file(s) in {dbf_dir}")
        return dbf_files
    
    def read_dbf_file(self, file_path: Path, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Read a .dbf file and return records as list of dictionaries
        
        Args:
            file_path: Path to the .dbf file
            limit: Maximum number of records to read (None for all)
        
        Returns:
            List of dictionaries containing the records
        """
        records = []
        try:
            logger.info(f"Reading DBF file: {file_path}")
            
            # Open in read-only mode to prevent corruption
            table = DBF(str(file_path), load=False, encoding='latin-1')
            
            # Read records
            for i, record in enumerate(table):
                if limit and i >= limit:
                    break
                    
                # Convert record to dictionary
                record_dict = dict(record)
                
                # Clean up the data (remove None values, strip strings)
                cleaned_record = {}
                for key, value in record_dict.items():
                    if value is not None:
                        if isinstance(value, str):
                            cleaned_record[key] = value.strip()
                        else:
                            cleaned_record[key] = value
                    else:
                        cleaned_record[key] = ""
                
                records.append(cleaned_record)
            
            logger.info(f"Successfully read {len(records)} records from {file_path}")
            
        except Exception as e:
            logger.error(f"Error reading DBF file {file_path}: {e}")
            raise
        
        return records
    
    def display_sample_data(self, records: List[Dict[str, Any]], sample_size: int = 10):
        """Display sample data from the records"""
        if not records:
            logger.warning("No records to display")
            return
        
        # Take sample
        sample_records = records[:sample_size]
        
        # Convert to pandas DataFrame for better display
        df = pd.DataFrame(sample_records)
        
        print("\n" + "=" * 80)
        print(f"SAMPLE DATA (First {min(sample_size, len(records))} records)")
        print("=" * 80)
        
        # Display DataFrame info
        print(f"\nTotal Records: {len(records)}")
        print(f"Columns: {', '.join(df.columns.tolist())}")
        print(f"\nData Types:")
        print(df.dtypes)
        
        print(f"\nSample Records:")
        print("-" * 80)
        for i, record in enumerate(sample_records, 1):
            print(f"\nRecord {i}:")
            for key, value in record.items():
                print(f"  {key:20s}: {value}")
        
        # Also log the sample
        logger.info(f"Sample data displayed: {sample_size} records shown")
        
    def get_dbf_schema(self, file_path: Path) -> Dict[str, str]:
        """Get the schema (field names and types) of a DBF file"""
        try:
            table = DBF(str(file_path), load=False)
            schema = {}
            
            for field in table.fields:
                schema[field.name] = {
                    'type': field.type,
                    'length': field.length,
                    'decimal_count': field.decimal_count
                }
            
            logger.info(f"DBF Schema extracted: {len(schema)} fields")
            return schema
            
        except Exception as e:
            logger.error(f"Error getting DBF schema: {e}")
            raise


def main():
    """Main function to demonstrate DBF reading capability"""
    print("\n" + "=" * 80)
    print("ESL INVENTORY SYNCHRONIZATION MIDDLEWARE - STEP 1: DBF READER")
    print("=" * 80)
    
    # Initialize configuration
    config = Config()
    
    # Create sample config file if it doesn't exist
    if not os.path.exists("config.json"):
        config.save_to_file()
        print(f"\n✓ Created default configuration file: config.json")
    
    # Initialize DBF Reader
    reader = DBFReader(config)
    
    # Find DBF files
    dbf_files = reader.find_dbf_files()
    
    if not dbf_files:
        print(f"\n⚠ No DBF files found in {config.DBF_INPUT_DIR}")
        print("Please place your .dbf files in the input directory and try again.")
        
        # Create a sample DBF file for testing
        print("\nWould you like to create a sample DBF file for testing? (y/n): ", end="")
        if input().lower() == 'y':
            create_sample_dbf(config.DBF_INPUT_DIR)
            dbf_files = reader.find_dbf_files()
    
    # Process first DBF file found
    if dbf_files:
        first_file = dbf_files[0]
        print(f"\n✓ Processing: {first_file}")
        
        # Get and display schema
        print("\n" + "-" * 80)
        print("DBF FILE SCHEMA:")
        print("-" * 80)
        schema = reader.get_dbf_schema(first_file)
        for field_name, field_info in schema.items():
            print(f"  {field_name:20s}: Type={field_info}, Length=N/A")
        
        # Read and display sample records
        records = reader.read_dbf_file(first_file, limit=100)  # Read up to 100 records
        reader.display_sample_data(records, sample_size=10)
        
        print("\n✓ Step 1 Complete: DBF Reader successfully implemented!")
        print(f"✓ Read {len(records)} records from {first_file.name}")
        
    else:
        print("\n✗ No DBF files available for processing.")
        

def create_sample_dbf(output_dir: str):
    """Create a sample DBF file for testing (requires dbfwrite or similar)"""
    print("\n⚠ Sample DBF creation requires additional libraries.")
    print("For testing, please provide an actual .dbf file from your POS system.")
    print(f"Place it in: {output_dir}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n✓ Program terminated by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        print(f"\n✗ Error: {e}")
        sys.exit(1)
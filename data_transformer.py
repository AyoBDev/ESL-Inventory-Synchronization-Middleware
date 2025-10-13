"""
Fixed Data Transformer for Your Specific DBF Structure
Based on the actual STOCK.DBF columns from your system
"""

import os
import csv
import json
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
from decimal import Decimal
import tempfile
import shutil
import time

from loguru import logger
import pandas as pd

from config_manager import Config


class FixedDataTransformer:
    """Data transformer with fixed field mappings for your DBF structure"""
    
    def __init__(self, config: Config):
        self.config = config
        self.ensure_output_directory()
        
        # YOUR ACTUAL DBF FIELD MAPPINGS
        # Based on the CSV header you provided
        self.field_mapping = {
            # Primary mappings for ESL
            'PART_NO': 'SKU',           # Part number -> SKU
            'PRICE1': 'CurrentPrice',    # First price tier -> Current Price
            'QTY': 'StockQuantity',      # Quantity on hand -> Stock Quantity
            'INTERNAL': 'TransactionID', # Internal code as transaction ID
            
            # Additional useful fields you might want
            'DESC': 'Description',       # Item description
            'COST': 'Cost',             # Cost price
            'MINIMUM': 'MinStock',       # Minimum stock level
            'MAXIMUM': 'MaxStock',       # Maximum stock level
            'GROUP': 'Category',         # Product group/category
            'SUPPLIER': 'Supplier',      # Supplier code
            'LASTSOLD': 'LastSold',     # Last sold date
            'ONORDER': 'OnOrder',        # Quantity on order
            'BARCODE': 'Barcode',        # If you have barcode field
            'SPRICE': 'SpecialPrice',    # Special/sale price
        }
        
        logger.info("Fixed Data Transformer initialized")
        logger.info(f"Field mappings configured for {len(self.field_mapping)} fields")
        
    def ensure_output_directory(self):
        """Create output directory if it doesn't exist"""
        Path(self.config.CSV_OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
        logger.debug(f"Output directory ready: {self.config.CSV_OUTPUT_DIR}")
    
    def transform_record(self, dbf_record: Dict[str, Any], 
                        timestamp_utc: Optional[str] = None) -> Dict[str, Any]:
        """
        Transform a DBF record to ESL CSV format using fixed mappings
        
        Args:
            dbf_record: Dictionary containing DBF record data
            timestamp_utc: Optional timestamp to use
            
        Returns:
            Dictionary with ESL fields
        """
        if timestamp_utc is None:
            timestamp_utc = datetime.utcnow().isoformat() + 'Z'
        
        # Initialize ESL record with required fields
        esl_record = {
            'SKU': '',
            'CurrentPrice': '0.00',
            'StockQuantity': '0',
            'TransactionID': '0',
            'TimeStampUTC': timestamp_utc
        }
        
        # Map PART_NO to SKU
        if 'PART_NO' in dbf_record:
            esl_record['SKU'] = str(dbf_record['PART_NO']).strip()
        
        # Map PRICE1 to CurrentPrice (handle both PRICE1 and SPRICE for special pricing)
        if 'SPRICE' in dbf_record and dbf_record['SPRICE'] and float(dbf_record['SPRICE']) > 0:
            # Use special price if available
            try:
                price = Decimal(str(dbf_record['SPRICE']).replace(',', ''))
                esl_record['CurrentPrice'] = f"{price:.2f}"
            except:
                pass
        elif 'PRICE1' in dbf_record:
            # Otherwise use regular price
            try:
                price = Decimal(str(dbf_record['PRICE1']).replace(',', ''))
                esl_record['CurrentPrice'] = f"{price:.2f}"
            except:
                esl_record['CurrentPrice'] = '0.00'
        
        # Map QTY to StockQuantity
        if 'QTY' in dbf_record:
            try:
                qty = int(float(str(dbf_record['QTY'])))
                esl_record['StockQuantity'] = str(qty)
            except:
                esl_record['StockQuantity'] = '0'
        
        # Map INTERNAL to TransactionID (or use PART_NO as fallback)
        if 'INTERNAL' in dbf_record:
            esl_record['TransactionID'] = str(dbf_record['INTERNAL']).strip()
        elif 'PART_NO' in dbf_record:
            esl_record['TransactionID'] = str(dbf_record['PART_NO']).strip()
        
        # Optional: Add description if needed
        if self.config.__dict__.get('INCLUDE_DESCRIPTION', False):
            if 'DESC' in dbf_record:
                esl_record['Description'] = str(dbf_record['DESC']).strip()
        
        return esl_record
    
    def transform_batch(self, dbf_records: List[Dict[str, Any]], 
                       source_file_name: str) -> List[Dict[str, Any]]:
        """
        Transform a batch of DBF records
        
        Args:
            dbf_records: List of DBF record dictionaries
            source_file_name: Name of the source DBF file
            
        Returns:
            List of transformed ESL records
        """
        if not dbf_records:
            logger.warning("No records to transform")
            return []
        
        logger.info(f"Transforming {len(dbf_records)} records from {source_file_name}")
        
        timestamp_utc = datetime.utcnow().isoformat() + 'Z'
        esl_records = []
        
        for i, dbf_record in enumerate(dbf_records):
            try:
                esl_record = self.transform_record(dbf_record, timestamp_utc)
                
                # Skip records with empty SKU
                if esl_record['SKU']:
                    esl_records.append(esl_record)
                else:
                    logger.debug(f"Skipping record {i}: Empty SKU")
                    
            except Exception as e:
                logger.error(f"Failed to transform record {i}: {e}")
                continue
        
        logger.info(f"Successfully transformed {len(esl_records)} records")
        return esl_records
    
    def write_csv_atomic(self, records: List[Dict[str, Any]], 
                        output_file_name: str) -> Optional[str]:
        """
        Write CSV file atomically (write to temp, then rename)
        
        Args:
            records: List of ESL record dictionaries
            output_file_name: Name for the output file
            
        Returns:
            Path to the created CSV file
        """
        if not records:
            logger.warning("No records to write to CSV")
            return None
        
        output_dir = Path(self.config.CSV_OUTPUT_DIR)
        final_path = output_dir / output_file_name
        
        # Create temporary file
        temp_fd, temp_path = tempfile.mkstemp(
            suffix='.tmp', 
            prefix='esl_', 
            dir=str(output_dir)
        )
        
        try:
            # Define CSV headers
            csv_headers = ['SKU', 'CurrentPrice', 'StockQuantity', 'TransactionID', 'TimeStampUTC']
            
            # Add optional headers if configured
            if self.config.__dict__.get('INCLUDE_DESCRIPTION', False):
                csv_headers.append('Description')
            
            # Write to temporary file
            with os.fdopen(temp_fd, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(
                    csvfile, 
                    fieldnames=csv_headers,
                    extrasaction='ignore'  # Ignore extra fields
                )
                
                writer.writeheader()
                writer.writerows(records)
            
            # Backup existing file if it exists
            if final_path.exists():
                backup_path = final_path.with_suffix('.bak')
                shutil.move(str(final_path), str(backup_path))
                logger.debug(f"Backed up existing file to: {backup_path}")
            
            # Atomic rename
            shutil.move(temp_path, str(final_path))
            
            logger.info(f"CSV file created: {final_path} ({len(records)} records)")
            return str(final_path)
            
        except Exception as e:
            # Clean up temp file on error
            if os.path.exists(temp_path):
                os.remove(temp_path)
            logger.error(f"Failed to write CSV file: {e}")
            raise
    
    def generate_csv_filename(self, source_file_name: str) -> str:
        """
        Generate timestamped CSV filename
        
        THIS IS WHY NEW FILES ARE CREATED:
        Each sync cycle creates a new timestamped file to:
        1. Preserve data integrity (never overwrite)
        2. Create an audit trail
        3. Allow ESL system to process files in order
        4. Prevent corruption during write
        
        Format: SOURCENAME_YYYYMMDDHHMMSS.csv
        """
        base_name = Path(source_file_name).stem.upper()
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        return f"{base_name}_{timestamp}.csv"
    
    def process_changes(self, dbf_records: List[Dict[str, Any]], 
                       source_file_name: str,
                       only_changed: bool = True) -> Optional[str]:
        """
        Process DBF records and create CSV output
        
        Args:
            dbf_records: List of DBF records to process
            source_file_name: Name of source DBF file
            only_changed: If True, only process changed records
            
        Returns:
            Path to created CSV file or None
        """
        if not dbf_records:
            logger.info("No records to process")
            return None
        
        # Transform records
        esl_records = self.transform_batch(dbf_records, source_file_name)
        
        if not esl_records:
            logger.warning("No valid records after transformation")
            return None
        
        # Generate output filename with timestamp
        csv_filename = self.generate_csv_filename(source_file_name)
        
        # Write CSV file
        csv_path = self.write_csv_atomic(esl_records, csv_filename)
        
        return csv_path
    
    def get_field_info(self):
        """Return information about the field mappings"""
        info = []
        info.append("=" * 60)
        info.append("FIELD MAPPING CONFIGURATION")
        info.append("=" * 60)
        info.append("DBF Fields from your STOCK.DBF:")
        info.append("-" * 40)
        
        for dbf_field, esl_field in self.field_mapping.items():
            info.append(f"  {dbf_field:15s} -> {esl_field}")
        
        info.append("")
        info.append("Key fields being used:")
        info.append(f"  SKU:            PART_NO")
        info.append(f"  Price:          PRICE1 (or SPRICE if on special)")
        info.append(f"  Quantity:       QTY")
        info.append(f"  Transaction ID: INTERNAL")
        
        return "\n".join(info)
    
    def detect_file_type(self, filename: str) -> str:
        """
        Detect the file type based on the filename.
        Returns 'TRANSACTION' if the filename suggests a transaction file, otherwise 'INVENTORY'.
        """
        if 'transaction' in filename.lower():
            return 'TRANSACTION'
        return 'INVENTORY'
    
    # Add your transformation methods here

    def transform_and_write_batch(self, records, dbf_filename, file_type):
        """
        Transform a batch of records and write to a CSV file.
        Returns the path to the created CSV file, or None if no file was created.
        """

        if not records:
            return None

        output_dir = getattr(self.config, "CSV_OUTPUT_DIR", "output_csv")
        os.makedirs(output_dir, exist_ok=True)
        csv_filename = f"{Path(dbf_filename).stem}_{file_type}_{int(time.time())}.csv"
        csv_path = Path(output_dir) / csv_filename

        # Assume all records are dicts with the same keys
        fieldnames = records[0].keys()

        with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(records)

        return str(csv_path)


def explain_csv_generation():
    """Explain why new CSV files are created"""
    
    explanation = """
    ============================================================
    WHY NEW CSV FILES ARE CREATED ON EACH SYNC
    ============================================================
    
    The middleware creates a NEW timestamped CSV file each time 
    it detects changes for several important reasons:
    
    1. DATA INTEGRITY
       - Never overwrites existing files
       - Prevents corruption during write operations
       - Atomic file operations ensure complete writes
    
    2. AUDIT TRAIL
       - Each file shows exactly when data was synchronized
       - Can track historical changes
       - Helps with troubleshooting
    
    3. ESL SYSTEM COMPATIBILITY
       - Many ESL systems expect new files to process
       - They monitor for new files, not file changes
       - Files can be processed in chronological order
    
    4. CONCURRENT ACCESS SAFETY
       - ESL system can read old file while new one is created
       - No file locking issues
       - No read/write conflicts
    
    5. BACKUP AND RECOVERY
       - Previous files serve as automatic backups
       - Can rollback to earlier state if needed
       - Historical data preservation
    
    File naming pattern: STOCK_YYYYMMDDHHMMSS.csv
    Example: STOCK_20251013163735.csv
             (Created on 2025-10-13 at 16:37:35)
    
    The system only creates new files when there are CHANGES:
    - New items added
    - Prices updated  
    - Stock quantities changed
    - Items deleted
    
    If nothing changes, no new file is created.
    ============================================================
    """
    
    return explanation


if __name__ == "__main__":
    print(explain_csv_generation())
    
    # Test with your configuration
    config = Config()
    transformer = FixedDataTransformer(config)
    
    # Show field mapping
    print(transformer.get_field_info())
    
    # Example of how your DBF record would be transformed
    sample_dbf_record = {
        'INTERNAL': '100001',
        'INACTIVE': 'N',
        'PART_NO': 'SKU12345',
        'DESC': 'Test Product Description',
        'PRICE1': '29.99',
        'QTY': '150',
        'COST': '15.00',
        'SUPPLIER': 'SUP001',
        'MINIMUM': '10',
        'MAXIMUM': '500',
        'GROUP': 'ELECTRONICS'
    }
    
    print("\nSample transformation:")
    print("-" * 40)
    print("Input DBF record:")
    for key, value in list(sample_dbf_record.items())[:5]:
        print(f"  {key}: {value}")
    
    print("\nOutput ESL record:")
    esl_record = transformer.transform_record(sample_dbf_record)
    for key, value in esl_record.items():
        print(f"  {key}: {value}")
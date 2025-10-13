"""
ESL Inventory Synchronization Middleware - Step 3: Data Transformation
Transforms DBF records to ESL CSV format with atomic file writing
"""

import os
import csv
import json
from pathlib import Path
from typing import Dict, List, Any, Optional, Callable
from datetime import datetime
from dataclasses import dataclass, field
from decimal import Decimal
import tempfile
import shutil

from loguru import logger
import pandas as pd

# Import from previous steps
# from dbf_reader import Config
from dbf_reader_with_memo import Config, EnhancedDBFReader as DBFReader
from incremental_detector import IncrementalDetector, StateTracker, ChangeType


@dataclass
class ESLRecord:
    """ESL CSV record structure"""
    SKU: str
    CurrentPrice: Decimal
    StockQuantity: int
    TransactionID: str
    TimeStampUTC: str
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for CSV writing"""
        return {
            'SKU': self.SKU,
            'CurrentPrice': f"{self.CurrentPrice:.2f}",
            'StockQuantity': str(self.StockQuantity),
            'TransactionID': self.TransactionID,
            'TimeStampUTC': self.TimeStampUTC
        }
    
    @classmethod
    def get_csv_headers(cls) -> List[str]:
        """Get CSV header names in order"""
        return ['SKU', 'CurrentPrice', 'StockQuantity', 'TransactionID', 'TimeStampUTC']


@dataclass
class FieldMapping:
    """Defines mapping between DBF fields and ESL CSV fields"""
    dbf_field: str
    esl_field: str
    transform_func: Optional['Callable[[Any], Any]'] = None
    default_value: Any = None
    
    def apply(self, value: Any) -> Any:
        """Apply transformation to the value"""
        if value is None or value == '':
            return self.default_value
        
        if self.transform_func:
            try:
                return self.transform_func(value)
            except Exception as e:
                logger.warning(f"Transform failed for {self.dbf_field}->{self.esl_field}: {e}")
                return self.default_value
        
        return value


class DataTransformer:
    """Transforms DBF records to ESL CSV format"""
    
    def __init__(self, config: Config):
        self.config = config
        self.setup_mappings()
        self.ensure_output_directory()
        
    def setup_mappings(self):
        """Define field mappings from DBF to ESL CSV"""
        
        # Transform functions
        def to_decimal(value):
            """Convert to decimal with 2 decimal places"""
            if isinstance(value, (int, float)):
                return Decimal(str(value)).quantize(Decimal('0.01'))
            elif isinstance(value, str):
                # Clean the string (remove currency symbols, spaces)
                cleaned = value.replace('$', '').replace(',', '').strip()
                return Decimal(cleaned).quantize(Decimal('0.01'))
            return Decimal('0.00')
        
        def to_integer(value):
            """Convert to integer, handling various formats"""
            if isinstance(value, (int, float)):
                return int(value)
            elif isinstance(value, str):
                # Handle negative numbers and clean the string
                cleaned = value.replace(',', '').strip()
                if cleaned.startswith('(') and cleaned.endswith(')'):
                    # Accounting format for negative: (123) = -123
                    cleaned = '-' + cleaned[1:-1]
                return int(float(cleaned))
            return 0
        
        def to_string(value):
            """Convert to string and clean"""
            return str(value).strip() if value is not None else ""
        
        # Define mappings for different file types
        self.stock_mappings = [
            FieldMapping('PART_NO', 'SKU', to_string, ''),
            FieldMapping('PART_NUMBER', 'SKU', to_string, ''),  # Alternative field name
            FieldMapping('PRICE', 'CurrentPrice', to_decimal, Decimal('0.00')),
            FieldMapping('SELL_PRICE', 'CurrentPrice', to_decimal, Decimal('0.00')),  # Alternative
            FieldMapping('STOCK', 'StockQuantity', to_integer, 0),
            FieldMapping('STOCK_QTY', 'StockQuantity', to_integer, 0),  # Alternative
            FieldMapping('STOCK_QUANTITY', 'StockQuantity', to_integer, 0),  # Alternative
            FieldMapping('DOC_NO', 'TransactionID', to_string, '0'),
            FieldMapping('DOCKET_NUMBER', 'TransactionID', to_string, '0'),  # Alternative
        ]
        
        self.transaction_mappings = [
            FieldMapping('PART_NO', 'SKU', to_string, ''),
            FieldMapping('ITEM_CODE', 'SKU', to_string, ''),  # Alternative for transactions
            FieldMapping('UNIT_PRICE', 'CurrentPrice', to_decimal, Decimal('0.00')),
            FieldMapping('QTY_SOLD', 'StockQuantity', to_integer, 0),
            FieldMapping('QUANTITY', 'StockQuantity', to_integer, 0),  # Alternative
            FieldMapping('DOC_NO', 'TransactionID', to_string, '0'),
            FieldMapping('INVOICE_NO', 'TransactionID', to_string, '0'),  # Alternative
        ]
        
    def ensure_output_directory(self):
        """Create output directory if it doesn't exist"""
        Path(self.config.CSV_OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
        logger.debug(f"Ensured output directory exists: {self.config.CSV_OUTPUT_DIR}")
    
    def detect_file_type(self, file_name: str) -> str:
        """Detect the type of DBF file based on name"""
        file_name_upper = file_name.upper()
        
        if 'STOCK' in file_name_upper or 'INVENTORY' in file_name_upper:
            return 'STOCK'
        elif 'INVOICE' in file_name_upper or 'TRANS' in file_name_upper or 'SALE' in file_name_upper:
            return 'TRANSACTION'
        else:
            # Default to stock type
            return 'STOCK'
    
    def get_mappings_for_file(self, file_type: str) -> List[FieldMapping]:
        """Get appropriate mappings based on file type"""
        if file_type == 'TRANSACTION':
            return self.transaction_mappings
        else:
            return self.stock_mappings
    
    def find_matching_field(self, record: Dict[str, Any], possible_fields: List[str]) -> Optional[str]:
        """Find the first matching field from a list of possible field names"""
        record_fields_upper = {k.upper(): k for k in record.keys()}
        
        for field in possible_fields:
            if field.upper() in record_fields_upper:
                return record_fields_upper[field.upper()]
        
        return ""
    
    def transform_record(self, dbf_record: Dict[str, Any], 
                        file_type: str = 'STOCK',
                        timestamp_utc: Optional[str] = None) -> ESLRecord:
        """
        Transform a DBF record to ESL CSV format
        
        Args:
            dbf_record: Dictionary containing DBF record data
            file_type: Type of file (STOCK or TRANSACTION)
            timestamp_utc: Optional timestamp to use
        
        Returns:
            ESLRecord object
        """
        if timestamp_utc is None:
            timestamp_utc = datetime.utcnow().isoformat() + 'Z'
        
        mappings = self.get_mappings_for_file(file_type)
        transformed = {}
        
        # Create uppercase key mapping for case-insensitive matching
        record_keys_upper = {k.upper(): k for k in dbf_record.keys()}
        
        for mapping in mappings:
            # Try to find the field (case-insensitive)
            actual_field = record_keys_upper.get(mapping.dbf_field.upper())
            
            if actual_field and actual_field in dbf_record:
                value = dbf_record[actual_field]
                transformed[mapping.esl_field] = mapping.apply(value)
            elif mapping.esl_field not in transformed:
                # Use default value if field not found
                transformed[mapping.esl_field] = mapping.default_value
        
        # Handle SKU field with multiple possible names
        if not transformed.get('SKU'):
            sku_field = self.find_matching_field(
                dbf_record, 
                ['PART_NO', 'PART_NUMBER', 'ITEM_CODE', 'PRODUCT_CODE', 'SKU']
            )
            if sku_field:
                transformed['SKU'] = str(dbf_record[sku_field]).strip()
        
        # Handle Price field with multiple possible names
        if transformed.get('CurrentPrice') == Decimal('0.00'):
            price_field = self.find_matching_field(
                dbf_record,
                ['PRICE', 'SELL_PRICE', 'UNIT_PRICE', 'RETAIL_PRICE']
            )
            if price_field:
                transformed['CurrentPrice'] = FieldMapping(
                    price_field, 'CurrentPrice', 
                    lambda v: Decimal(str(v).replace('$', '').replace(',', '')).quantize(Decimal('0.01')),
                    Decimal('0.00')
                ).apply(dbf_record[price_field])
        
        # Create ESLRecord
        return ESLRecord(
            SKU=transformed.get('SKU', ''),
            CurrentPrice=transformed.get('CurrentPrice', Decimal('0.00')),
            StockQuantity=transformed.get('StockQuantity', 0),
            TransactionID=transformed.get('TransactionID', '0'),
            TimeStampUTC=timestamp_utc
        )
    
    def write_csv_atomic(self, records: List[ESLRecord], 
                        output_file_name: str) -> str:
        """
        Write CSV file atomically (write to temp, then rename)
        
        Args:
            records: List of ESLRecord objects
            output_file_name: Name for the output file
        
        Returns:
            Path to the created CSV file
        """
        output_dir = Path(self.config.CSV_OUTPUT_DIR)
        final_path = output_dir / output_file_name
        
        # Create temporary file in the same directory (for atomic rename)
        temp_fd, temp_path = tempfile.mkstemp(
            suffix='.tmp', 
            prefix='esl_', 
            dir=str(output_dir)
        )
        
        try:
            # Write to temporary file
            with os.fdopen(temp_fd, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(
                    csvfile, 
                    fieldnames=ESLRecord.get_csv_headers(),
                    lineterminator='\n'  # Ensure consistent line endings
                )
                
                # Write header
                writer.writeheader()
                
                # Write records
                for record in records:
                    writer.writerow(record.to_dict())
            
            # Atomic rename (this is atomic on Windows NTFS)
            if final_path.exists():
                # Backup existing file
                backup_path = final_path.with_suffix('.bak')
                shutil.move(str(final_path), str(backup_path))
                logger.info(f"Backed up existing file to: {backup_path}")
            
            # Rename temp file to final name
            shutil.move(temp_path, str(final_path))
            
            logger.info(f"CSV file created: {final_path} ({len(records)} records)")
            return str(final_path)
            
        except Exception as e:
            # Clean up temp file if something went wrong
            if os.path.exists(temp_path):
                os.remove(temp_path)
            logger.error(f"Failed to write CSV file: {e}")
            raise
    
    def generate_csv_filename(self, source_file_name: str) -> str:
        """
        Generate timestamped CSV filename
        Format: SOURCENAME_YYYYMMDDHHMMSS.csv
        
        Args:
            source_file_name: Name of the source DBF file
        
        Returns:
            Generated CSV filename
        """
        # Remove extension from source file
        base_name = Path(source_file_name).stem.upper()
        
        # Generate timestamp
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        
        # Create filename
        csv_filename = f"{base_name}_{timestamp}.csv"
        
        return csv_filename
    
    def transform_and_write_batch(self, dbf_records: List[Dict[str, Any]], 
                                 source_file_name: str,
                                 file_type: Optional[str] = None) -> str:
        """
        Transform a batch of DBF records and write to CSV
        
        Args:
            dbf_records: List of DBF record dictionaries
            source_file_name: Name of the source DBF file
            file_type: Optional file type override
        
        Returns:
            Path to created CSV file
        """
        if not dbf_records:
            logger.warning("No records to transform")
            return ""
        
        # Detect file type if not provided
        if file_type is None:
            file_type = self.detect_file_type(source_file_name)
        
        logger.info(f"Transforming {len(dbf_records)} records from {source_file_name} (type: {file_type})")
        
        # Transform all records
        timestamp_utc = datetime.utcnow().isoformat() + 'Z'
        esl_records = []
        
        for dbf_record in dbf_records:
            try:
                esl_record = self.transform_record(dbf_record, file_type, timestamp_utc)
                esl_records.append(esl_record)
            except Exception as e:
                logger.error(f"Failed to transform record: {e}")
                logger.debug(f"Record data: {dbf_record}")
                continue
        
        if not esl_records:
            logger.warning("No records successfully transformed")
            return ""
        
        # Generate output filename
        csv_filename = self.generate_csv_filename(source_file_name)
        
        # Write CSV file atomically
        csv_path = self.write_csv_atomic(esl_records, csv_filename)
        
        logger.info(f"Successfully transformed {len(esl_records)} records to {csv_path}")
        
        return csv_path


def demonstrate_transformation():
    """Demonstrate the data transformation capabilities"""
    print("\n" + "=" * 80)
    print("ESL MIDDLEWARE - STEP 3: DATA TRANSFORMATION TO CSV")
    print("=" * 80)
    
    # Initialize components
    config = Config()
    state_tracker = StateTracker()
    detector = IncrementalDetector(config, state_tracker)
    transformer = DataTransformer(config)
    
    # Find DBF files
    dbf_files = detector.dbf_reader.find_dbf_files()
    
    if not dbf_files:
        print(f"\n⚠ No DBF files found in {config.DBF_INPUT_DIR}")
        print("\nCreating sample data for demonstration...")
        create_sample_data(config)
        return
    
    # Process each DBF file
    for dbf_file in dbf_files:
        print(f"\n" + "-" * 80)
        print(f"Processing: {dbf_file[0].name}")
        print("-" * 80)
        
        # Detect file type
        file_type = transformer.detect_file_type(dbf_file[0].name)
        print(f"📁 File Type: {file_type}")
        
        # Determine ID field
        id_field = 'DOC_NO' if file_type == 'TRANSACTION' else 'PART_NO'
        
        # Detect changes
        print(f"\n🔍 Detecting changes...")
        changes = detector.detect_changes(dbf_file[0], id_field=id_field)
        
        # Get records that need synchronization
        sync_records = []
        
        # Combine new and updated records
        for item in changes['new']:
            sync_records.append(item['record'])
        
        for item in changes['updated']:
            sync_records.append(item['record'])
        
        if not sync_records:
            print(f"✓ No changes to synchronize")
            continue
        
        print(f"\n📊 Records to synchronize: {len(sync_records)}")
        
        # Show sample transformation
        if sync_records:
            print(f"\n📋 Sample transformation (first record):")
            sample_record = sync_records[0]
            
            print(f"\nOriginal DBF Record:")
            for key, value in list(sample_record.items())[:5]:  # Show first 5 fields
                print(f"  {key:20s}: {value}")
            
            # Transform single record for display
            esl_record = transformer.transform_record(sample_record, file_type)
            
            print(f"\nTransformed ESL Record:")
            for key, value in esl_record.to_dict().items():
                print(f"  {key:20s}: {value}")
        
        # Transform and write all records
        print(f"\n💾 Writing CSV file...")
        csv_path = transformer.transform_and_write_batch(
            sync_records, 
            dbf_file[0].name,
            file_type
        )
        
        if csv_path:
            print(f"✅ CSV file created: {csv_path}")
            
            # Display first few lines of the CSV
            print(f"\n📄 CSV Preview:")
            print("-" * 60)
            with open(csv_path, 'r') as f:
                for i, line in enumerate(f):
                    if i < 5:  # Show first 5 lines
                        print(line.rstrip())
                    else:
                        print("...")
                        break
    
    # Summary
    csv_files = list(Path(config.CSV_OUTPUT_DIR).glob("*.csv"))
    print("\n" + "=" * 80)
    print(f"✅ Step 3 Complete: Data Transformation Implemented!")
    print(f"✅ CSV files in output directory: {len(csv_files)}")
    print(f"✅ Output directory: {config.CSV_OUTPUT_DIR}")
    print("✅ Ready for Step 4: Scheduling and Error Handling")
    print("=" * 80)


def create_sample_data(config: Config):
    """Create sample data for testing"""
    sample_data = [
        {
            'SKU': 'TEST001',
            'CurrentPrice': '29.99',
            'StockQuantity': '100',
            'TransactionID': '1001',
            'TimeStampUTC': datetime.utcnow().isoformat() + 'Z'
        },
        {
            'SKU': 'TEST002',
            'CurrentPrice': '49.99',
            'StockQuantity': '50',
            'TransactionID': '1002',
            'TimeStampUTC': datetime.utcnow().isoformat() + 'Z'
        }
    ]
    
    # Create sample CSV
    output_file = Path(config.CSV_OUTPUT_DIR) / f"SAMPLE_{datetime.now():%Y%m%d%H%M%S}.csv"
    
    with open(output_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['SKU', 'CurrentPrice', 'StockQuantity', 'TransactionID', 'TimeStampUTC'])
        writer.writeheader()
        writer.writerows(sample_data)
    
    print(f"✅ Sample CSV created: {output_file}")


if __name__ == "__main__":
    try:
        demonstrate_transformation()
    except KeyboardInterrupt:
        print("\n\n✓ Program terminated by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
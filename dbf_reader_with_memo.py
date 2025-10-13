"""
Enhanced ESL Inventory Synchronization Middleware - DBF Reader with Memo Support
Handles DBF files with accompanying memo files (.FPT, .DBT, .MPT)
"""

import os
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import json
import struct

# Third-party imports
try:
    from dbfread import DBF
    from loguru import logger
    import pandas as pd
except ImportError as e:
    print(f"Missing required library: {e}")
    print("Please install: pip install dbfread loguru pandas")
    sys.exit(1)

from config_manager import Config


class MemoFieldInfo:
    """Information about memo fields in DBF files"""
    
    # Common memo field types in different DBF versions
    MEMO_EXTENSIONS = {
        '.FPT': 'FoxPro',      # Visual FoxPro memo
        '.DBT': 'dBase III/IV', # dBase III/IV memo
        '.MPT': 'FoxPro',       # FoxPro memo variant
        '.SMT': 'dBase IV',     # dBase IV system memo
    }
    
    # Field types that typically use memo files
    MEMO_FIELD_TYPES = ['M', 'G', 'P', 'B']  # Memo, General, Picture, Binary


class EnhancedDBFReader:
    """Enhanced DBF Reader that handles memo files"""
    
    def __init__(self, config: Config):
        self.config = config
        self.setup_logging()
        self.ensure_directories()
        self.memo_cache = {}  # Cache for memo file handles
        
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
        logger.info("Enhanced DBF Reader with Memo Support Started")
        logger.info(f"DBF Input Directory: {self.config.DBF_INPUT_DIR}")
        
    def ensure_directories(self):
        """Create necessary directories if they don't exist"""
        for dir_path in [self.config.DBF_INPUT_DIR, 
                         self.config.CSV_OUTPUT_DIR, 
                         self.config.LOG_DIR]:
            Path(dir_path).mkdir(parents=True, exist_ok=True)
            logger.debug(f"Ensured directory exists: {dir_path}")
    
    def find_dbf_files(self) -> List[Tuple[Path, Optional[Path]]]:
        """
        Find all DBF files and their associated memo files
        
        Returns:
            List of tuples (dbf_path, memo_path or None)
        """
        dbf_dir = Path(self.config.DBF_INPUT_DIR)
        dbf_files = list(dbf_dir.glob("*.dbf")) + list(dbf_dir.glob("*.DBF"))
        
        result = []
        for dbf_file in dbf_files:
            memo_file = self.find_memo_file(dbf_file)
            result.append((dbf_file, memo_file))
            
            if memo_file:
                logger.info(f"Found DBF with memo: {dbf_file.name} -> {memo_file.name}")
            else:
                logger.info(f"Found standalone DBF: {dbf_file.name}")
        
        logger.info(f"Total: {len(result)} DBF file(s), "
                   f"{sum(1 for _, m in result if m)} with memo files")
        
        return result
    
    def find_memo_file(self, dbf_path: Path) -> Optional[Path]:
        """
        Find the memo file associated with a DBF file
        
        Args:
            dbf_path: Path to the DBF file
            
        Returns:
            Path to memo file if found, None otherwise
        """
        base_name = dbf_path.stem
        
        # Check for memo files with common extensions
        for ext, memo_type in MemoFieldInfo.MEMO_EXTENSIONS.items():
            # Try both lowercase and uppercase extensions
            for memo_ext in [ext.lower(), ext.upper()]:
                memo_path = dbf_path.parent / f"{base_name}{memo_ext}"
                if memo_path.exists():
                    logger.debug(f"Found {memo_type} memo file: {memo_path.name}")
                    return memo_path
        
        return None
    
    def analyze_dbf_structure(self, file_path: Path) -> Dict[str, Any]:
        """
        Analyze DBF file structure including memo fields
        
        Args:
            file_path: Path to the DBF file
            
        Returns:
            Dictionary with file structure information
        """
        structure = {
            'file_name': file_path.name,
            'file_size': file_path.stat().st_size,
            'has_memo': False,
            'memo_file': None,
            'memo_fields': [],
            'regular_fields': [],
            'total_records': 0,
            'encoding': None,
            'version': None
        }
        
        try:
            # Open DBF to analyze structure
            table = DBF(str(file_path), load=False)
            
            structure['total_records'] = len(table)
            structure['encoding'] = table.encoding
            
            # Check for memo file
            memo_file = self.find_memo_file(file_path)
            if memo_file:
                structure['has_memo'] = True
                structure['memo_file'] = memo_file.name
            
            # Analyze fields
            for field in table.fields:
                field_info = {
                    'name': field.name,
                    'type': field.type,
                    'length': field.length,
                    'decimal_count': field.decimal_count
                }
                
                # Check if this is a memo field
                if field.type in MemoFieldInfo.MEMO_FIELD_TYPES:
                    structure['memo_fields'].append(field_info)
                    logger.debug(f"Memo field detected: {field.name} (type: {field.type})")
                else:
                    structure['regular_fields'].append(field_info)
            
            logger.info(f"Structure analysis for {file_path.name}:")
            logger.info(f"  Records: {structure['total_records']}")
            logger.info(f"  Regular fields: {len(structure['regular_fields'])}")
            logger.info(f"  Memo fields: {len(structure['memo_fields'])}")
            
        except Exception as e:
            logger.error(f"Error analyzing DBF structure: {e}")
            
        return structure
    
    def read_dbf_file(self, file_path: Path, 
                     limit: Optional[int] = None,
                     include_memo: bool = True) -> List[Dict[str, Any]]:
        """
        Read DBF file with memo field support
        
        Args:
            file_path: Path to the DBF file
            limit: Maximum number of records to read
            include_memo: Whether to include memo field contents
            
        Returns:
            List of dictionaries containing the records
        """
        records = []
        
        try:
            logger.info(f"Reading DBF file: {file_path}")
            
            # Check for memo file
            memo_file = self.find_memo_file(file_path) if include_memo else None
            
            # Configure DBF reader based on memo file presence
            if memo_file:
                logger.info(f"Reading with memo file: {memo_file.name}")
                # dbfread automatically handles memo files when they exist
                table = DBF(str(file_path), load=False, encoding='latin-1')
            else:
                table = DBF(str(file_path), load=False, encoding='latin-1')
            
            # Read records
            for i, record in enumerate(table):
                if limit and i >= limit:
                    break
                
                # Convert record to dictionary
                record_dict = dict(record)
                
                # Clean up the data
                cleaned_record = self.clean_record(record_dict)
                
                # Add metadata
                cleaned_record['_source_file'] = file_path.name
                cleaned_record['_has_memo'] = memo_file is not None
                cleaned_record['_record_index'] = i
                
                records.append(cleaned_record)
            
            logger.info(f"Successfully read {len(records)} records from {file_path}")
            
            # Log sample of memo field content if present
            if memo_file and records:
                self.log_memo_field_sample(records[0])
            
        except Exception as e:
            logger.error(f"Error reading DBF file {file_path}: {e}")
            raise
        
        return records
    
    def clean_record(self, record_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Clean and normalize record data, handling memo fields specially
        
        Args:
            record_dict: Raw record dictionary
            
        Returns:
            Cleaned record dictionary
        """
        cleaned = {}
        
        for key, value in record_dict.items():
            if value is None:
                cleaned[key] = ""
            elif isinstance(value, bytes):
                # Handle binary memo data
                try:
                    # Try to decode as text
                    cleaned[key] = value.decode('utf-8', errors='replace').strip()
                except:
                    # Store as hex string if binary
                    cleaned[key] = value.hex()
                    logger.debug(f"Binary memo field {key} converted to hex")
            elif isinstance(value, str):
                # Clean text, including memo text
                cleaned[key] = value.strip()
                # Truncate very long memo fields for display
                if len(cleaned[key]) > 1000:
                    logger.debug(f"Memo field {key} contains {len(cleaned[key])} characters")
            else:
                cleaned[key] = value
        
        return cleaned
    
    def log_memo_field_sample(self, record: Dict[str, Any]):
        """Log a sample of memo field content for debugging"""
        for key, value in record.items():
            if isinstance(value, str) and len(value) > 100:
                sample = value[:100] + "..." if len(value) > 100 else value
                logger.debug(f"Memo field '{key}' sample: {sample}")
    
    def export_memo_fields(self, records: List[Dict[str, Any]], 
                          output_file: str) -> None:
        """
        Export memo field contents to a separate file for analysis
        
        Args:
            records: List of records containing memo fields
            output_file: Path to output file
        """
        memo_data = []
        
        for record in records:
            memo_record = {
                '_record_index': record.get('_record_index', 0)
            }
            
            # Extract memo fields (typically long text fields)
            for key, value in record.items():
                if isinstance(value, str) and len(value) > 255:
                    memo_record[key] = value
            
            if len(memo_record) > 1:  # Has memo data besides index
                memo_data.append(memo_record)
        
        if memo_data:
            # Save memo data to JSON for analysis
            output_path = Path(self.config.CSV_OUTPUT_DIR) / output_file
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(memo_data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Exported {len(memo_data)} records with memo fields to {output_path}")
    
    def get_dbf_info(self, file_path: Path) -> Dict[str, Any]:
        """
        Get detailed information about a DBF file and its memo
        
        Args:
            file_path: Path to the DBF file
            
        Returns:
            Dictionary with detailed file information
        """
        info = {
            'dbf_file': file_path.name,
            'dbf_size': file_path.stat().st_size,
            'dbf_modified': datetime.fromtimestamp(file_path.stat().st_mtime).isoformat(),
            'memo_file': None,
            'memo_size': 0,
            'structure': self.analyze_dbf_structure(file_path)
        }
        
        memo_file = self.find_memo_file(file_path)
        if memo_file:
            info['memo_file'] = memo_file.name
            info['memo_size'] = memo_file.stat().st_size
            info['memo_modified'] = datetime.fromtimestamp(memo_file.stat().st_mtime).isoformat()
        
        return info
    
    def validate_memo_integrity(self, dbf_path: Path) -> Tuple[bool, List[str]]:
        """
        Validate that DBF and memo files are in sync
        
        Args:
            dbf_path: Path to the DBF file
            
        Returns:
            Tuple of (is_valid, list_of_issues)
        """
        issues = []
        
        memo_file = self.find_memo_file(dbf_path)
        if not memo_file:
            # No memo file expected
            return True, []
        
        # Check modification times
        dbf_mtime = dbf_path.stat().st_mtime
        memo_mtime = memo_file.stat().st_mtime
        
        # If times differ significantly, files might be out of sync
        if abs(dbf_mtime - memo_mtime) > 60:  # More than 1 minute difference
            issues.append(
                f"Modification time mismatch: DBF modified at "
                f"{datetime.fromtimestamp(dbf_mtime)}, "
                f"memo at {datetime.fromtimestamp(memo_mtime)}"
            )
        
        # Check if memo file is readable
        try:
            with open(memo_file, 'rb') as f:
                header = f.read(32)
                if len(header) < 32:
                    issues.append(f"Memo file appears corrupted (header too short)")
        except Exception as e:
            issues.append(f"Cannot read memo file: {e}")
        
        is_valid = len(issues) == 0
        
        if not is_valid:
            logger.warning(f"Memo validation issues for {dbf_path.name}: {issues}")
        
        return is_valid, issues


def demonstrate_memo_support():
    """Demonstrate the enhanced DBF reader with memo support"""
    print("\n" + "=" * 80)
    print("ENHANCED DBF READER - MEMO FILE SUPPORT")
    print("=" * 80)
    
    # Initialize configuration
    config = Config()
    
    # Initialize enhanced reader
    reader = EnhancedDBFReader(config)
    
    # Find all DBF files with their memo files
    dbf_files_with_memos = reader.find_dbf_files()
    
    if not dbf_files_with_memos:
        print(f"\n⚠ No DBF files found in {config.DBF_INPUT_DIR}")
        return
    
    # Process each DBF file
    for dbf_file, memo_file in dbf_files_with_memos:
        print(f"\n" + "-" * 80)
        print(f"Processing: {dbf_file.name}")
        
        if memo_file:
            print(f"  📎 Memo file: {memo_file.name}")
        else:
            print(f"  ℹ️  No memo file")
        
        # Get detailed information
        info = reader.get_dbf_info(dbf_file)
        
        print(f"\n📊 File Information:")
        print(f"  DBF Size: {info['dbf_size']:,} bytes")
        if info['memo_file']:
            print(f"  Memo Size: {info['memo_size']:,} bytes")
        
        # Analyze structure
        structure = info['structure']
        print(f"\n📋 Structure:")
        print(f"  Total Records: {structure['total_records']}")
        print(f"  Regular Fields: {len(structure['regular_fields'])}")
        print(f"  Memo Fields: {len(structure['memo_fields'])}")
        
        if structure['memo_fields']:
            print(f"\n  Memo Fields Detected:")
            for field in structure['memo_fields']:
                print(f"    - {field['name']} (Type: {field['type']})")
        
        # Validate memo integrity
        if memo_file:
            is_valid, issues = reader.validate_memo_integrity(dbf_file)
            if is_valid:
                print(f"\n✅ Memo file integrity: OK")
            else:
                print(f"\n⚠️  Memo file integrity issues:")
                for issue in issues:
                    print(f"    - {issue}")
        
        # Read sample records
        records = reader.read_dbf_file(dbf_file, limit=5, include_memo=True)
        
        if records and structure['memo_fields']:
            print(f"\n📝 Sample Memo Content (first record):")
            first_record = records[0]
            for memo_field in structure['memo_fields']:
                field_name = memo_field['name']
                if field_name in first_record:
                    content = first_record[field_name]
                    if content and len(str(content)) > 0:
                        preview = str(content)[:200] + "..." if len(str(content)) > 200 else str(content)
                        print(f"  {field_name}: {preview}")
        
        # Export memo fields if present
        if structure['memo_fields']:
            memo_output_file = f"{dbf_file.stem}_memo_fields.json"
            reader.export_memo_fields(records, memo_output_file)
            print(f"\n💾 Memo fields exported to: {memo_output_file}")
    
    print("\n" + "=" * 80)
    print("✅ Enhanced DBF reading with memo support complete!")
    print("=" * 80)


if __name__ == "__main__":
    try:
        demonstrate_memo_support()
    except KeyboardInterrupt:
        print("\n\n✓ Program terminated by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
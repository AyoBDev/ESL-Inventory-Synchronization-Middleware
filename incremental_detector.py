"""
ESL Inventory Synchronization Middleware - Step 2: Incremental Detection
Implements hybrid tracking strategy for detecting changes in DBF files
"""

import os
import json
import hashlib
from pathlib import Path
from typing import Dict, List, Set, Tuple, Any, Optional
from datetime import datetime
from dataclasses import dataclass, asdict
from enum import Enum

from dbfread import DBF
from loguru import logger
import pandas as pd

# Import from Step 1
from dbf_reader import Config, DBFReader


class ChangeType(Enum):
    """Types of changes detected in DBF records"""
    NEW = "NEW"
    UPDATED = "UPDATED"
    DELETED = "DELETED"
    UNCHANGED = "UNCHANGED"


@dataclass
class RecordState:
    """Represents the state of a single record for tracking"""
    record_id: str  # Usually PART_NO for stock, DOC_NO for transactions
    checksum: str
    last_seen: str  # ISO timestamp
    doc_no: Optional[int] = None  # For transaction tracking
    deleted: bool = False
    
    def to_dict(self) -> Dict:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'RecordState':
        return cls(**data)


class StateTracker:
    """Manages persistent state tracking for incremental synchronization"""
    
    def __init__(self, state_file: str = "state.json"):
        self.state_file = state_file
        self.state_data: Dict[str, Dict] = {}
        self.load_state()
        
    def load_state(self):
        """Load state from JSON file"""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    raw_data = json.load(f)
                    self.state_data = raw_data
                    logger.info(f"State loaded from {self.state_file}")
                    
                    # Log summary statistics
                    for file_name, file_state in self.state_data.items():
                        if isinstance(file_state, dict) and 'records' in file_state:
                            record_count = len(file_state['records'])
                            logger.info(f"  {file_name}: {record_count} records tracked")
            except Exception as e:
                logger.warning(f"Could not load state file: {e}. Starting fresh.")
                self.state_data = {}
        else:
            logger.info("No existing state file. Starting with empty state.")
            self.state_data = {}
    
    def save_state(self):
        """Save state to JSON file with atomic write"""
        temp_file = f"{self.state_file}.tmp"
        try:
            with open(temp_file, 'w') as f:
                json.dump(self.state_data, f, indent=2, default=str)
            
            # Atomic rename
            os.replace(temp_file, self.state_file)
            logger.debug(f"State saved to {self.state_file}")
            
        except Exception as e:
            logger.error(f"Failed to save state: {e}")
            if os.path.exists(temp_file):
                os.remove(temp_file)
            raise
    
    def get_file_state(self, file_name: str) -> Dict:
        """Get state for a specific file"""
        if file_name not in self.state_data:
            self.state_data[file_name] = {
                'last_processed': None,
                'last_doc_no': 0,
                'records': {},
                'file_checksum': None,
                'last_modified': None
            }
        return self.state_data[file_name]
    
    def update_file_state(self, file_name: str, updates: Dict):
        """Update state for a specific file"""
        file_state = self.get_file_state(file_name)
        file_state.update(updates)
        self.save_state()


class IncrementalDetector:
    """Detects incremental changes in DBF files using hybrid tracking"""
    
    def __init__(self, config: Config, state_tracker: StateTracker):
        self.config = config
        self.state_tracker = state_tracker
        self.dbf_reader = DBFReader(config)
        
    def calculate_record_checksum(self, record: Dict, 
                                 exclude_fields: Optional[List[str]] = None) -> str:
        """
        Calculate MD5 checksum of a record for change detection
        
        Args:
            record: Dictionary containing record data
            exclude_fields: Fields to exclude from checksum (e.g., timestamps)
        
        Returns:
            MD5 hex digest of the record
        """
        exclude_fields = exclude_fields or ['TIMESTAMP', 'MODIFIED']
        
        # Create a sorted, consistent representation of the record
        checksum_data = {}
        for key, value in sorted(record.items()):
            if key not in exclude_fields:
                # Normalize the value
                if value is None:
                    checksum_data[key] = ""
                elif isinstance(value, (int, float)):
                    checksum_data[key] = str(value)
                elif isinstance(value, str):
                    checksum_data[key] = value.strip()
                else:
                    checksum_data[key] = str(value)
        
        # Create hash
        data_str = json.dumps(checksum_data, sort_keys=True)
        return hashlib.md5(data_str.encode()).hexdigest()
    
    def detect_changes(self, file_path: Path, 
                      id_field: str = "PART_NO",
                      track_doc_no: bool = False) -> Dict[str, List[Dict]]:
        """
        Detect changes in a DBF file compared to last known state
        
        Args:
            file_path: Path to the DBF file
            id_field: Primary key field for records (PART_NO for stock, DOC_NO for transactions)
            track_doc_no: Whether to track DOC_NO for transaction ordering
        
        Returns:
            Dictionary with 'new', 'updated', 'deleted', 'unchanged' record lists
        """
        file_name = file_path.name
        file_state = self.state_tracker.get_file_state(file_name)
        previous_records = file_state.get('records', {})
        
        changes = {
            'new': [],
            'updated': [],
            'deleted': [],
            'unchanged': []
        }
        
        current_record_ids = set()
        current_timestamp = datetime.now().isoformat()
        max_doc_no = file_state.get('last_doc_no', 0)
        
        try:
            logger.info(f"Detecting changes in {file_name}")
            
            # Read current records
            current_records = self.dbf_reader.read_dbf_file(file_path)
            
            for record in current_records:
                # Get record identifier
                record_id = str(record.get(id_field, ""))
                if not record_id:
                    logger.warning(f"Record missing {id_field}, skipping")
                    continue
                
                current_record_ids.add(record_id)
                
                # Track DOC_NO if applicable
                if track_doc_no and 'DOC_NO' in record:
                    try:
                        doc_no = int(record.get('DOC_NO', 0))
                        max_doc_no = max(max_doc_no, doc_no)
                    except (ValueError, TypeError):
                        pass
                
                # Calculate checksum
                checksum = self.calculate_record_checksum(record)
                
                # Check if record exists in previous state
                if record_id in previous_records:
                    prev_state = RecordState.from_dict(previous_records[record_id])
                    
                    if prev_state.checksum != checksum:
                        # Record has been updated
                        changes['updated'].append({
                            'record': record,
                            'change_type': ChangeType.UPDATED,
                            'record_id': record_id,
                            'old_checksum': prev_state.checksum,
                            'new_checksum': checksum
                        })
                        
                        # Update state
                        previous_records[record_id] = RecordState(
                            record_id=record_id,
                            checksum=checksum,
                            last_seen=current_timestamp,
                            doc_no=record.get('DOC_NO')
                        ).to_dict()
                    else:
                        # Record unchanged
                        changes['unchanged'].append({
                            'record': record,
                            'change_type': ChangeType.UNCHANGED,
                            'record_id': record_id
                        })
                        
                        # Update last_seen
                        previous_records[record_id]['last_seen'] = current_timestamp
                else:
                    # New record
                    changes['new'].append({
                        'record': record,
                        'change_type': ChangeType.NEW,
                        'record_id': record_id,
                        'checksum': checksum
                    })
                    
                    # Add to state
                    previous_records[record_id] = RecordState(
                        record_id=record_id,
                        checksum=checksum,
                        last_seen=current_timestamp,
                        doc_no=record.get('DOC_NO')
                    ).to_dict()
            
            # Detect deleted records (in previous state but not in current)
            for record_id in previous_records:
                if record_id not in current_record_ids:
                    prev_state = previous_records[record_id]
                    if not prev_state.get('deleted', False):
                        changes['deleted'].append({
                            'record_id': record_id,
                            'change_type': ChangeType.DELETED,
                            'last_state': prev_state
                        })
                        
                        # Mark as deleted
                        previous_records[record_id]['deleted'] = True
            
            # Update file state
            self.state_tracker.update_file_state(file_name, {
                'last_processed': current_timestamp,
                'last_doc_no': max_doc_no,
                'records': previous_records,
                'last_modified': datetime.fromtimestamp(
                    file_path.stat().st_mtime
                ).isoformat()
            })
            
            # Log summary
            logger.info(f"Change detection complete for {file_name}:")
            logger.info(f"  New records: {len(changes['new'])}")
            logger.info(f"  Updated records: {len(changes['updated'])}")
            logger.info(f"  Deleted records: {len(changes['deleted'])}")
            logger.info(f"  Unchanged records: {len(changes['unchanged'])}")
            logger.info(f"  Max DOC_NO: {max_doc_no}")
            
        except Exception as e:
            logger.error(f"Error detecting changes in {file_path}: {e}")
            raise
        
        return changes
    
    def get_changed_records_for_sync(self, changes: Dict[str, List[Dict]]) -> List[Dict]:
        """
        Get list of records that need to be synchronized (new + updated)
        
        Args:
            changes: Dictionary of changes from detect_changes()
        
        Returns:
            List of records that need synchronization
        """
        sync_records = []
        
        # Add new records
        for item in changes['new']:
            record = item['record'].copy()
            record['_sync_action'] = 'INSERT'
            record['_sync_timestamp'] = datetime.now().isoformat()
            sync_records.append(record)
        
        # Add updated records
        for item in changes['updated']:
            record = item['record'].copy()
            record['_sync_action'] = 'UPDATE'
            record['_sync_timestamp'] = datetime.now().isoformat()
            sync_records.append(record)
        
        # Note: Deleted records might need special handling depending on ESL requirements
        # For now, we'll include them with a DELETE action
        for item in changes['deleted']:
            sync_records.append({
                '_sync_action': 'DELETE',
                '_sync_timestamp': datetime.now().isoformat(),
                '_record_id': item['record_id']
            })
        
        return sync_records


def demonstrate_incremental_detection():
    """Demonstrate incremental detection capabilities"""
    print("\n" + "=" * 80)
    print("ESL MIDDLEWARE - STEP 2: INCREMENTAL DETECTION")
    print("=" * 80)
    
    # Initialize components
    config = Config()
    state_tracker = StateTracker()
    detector = IncrementalDetector(config, state_tracker)
    
    # Find DBF files
    dbf_files = detector.dbf_reader.find_dbf_files()
    
    if not dbf_files:
        print(f"\n⚠ No DBF files found in {config.DBF_INPUT_DIR}")
        return
    
    # Process each DBF file
    for dbf_file in dbf_files:
        print(f"\n" + "-" * 80)
        print(f"Processing: {dbf_file.name}")
        print("-" * 80)
        
        # Determine file type and ID field
        file_name_upper = dbf_file.name.upper()
        if 'STOCK' in file_name_upper:
            id_field = 'PART_NO'
            track_doc = False
            print(f"File Type: Stock/Inventory (ID field: {id_field})")
        elif 'INVOICE' in file_name_upper or 'TRANS' in file_name_upper:
            id_field = 'DOC_NO'
            track_doc = True
            print(f"File Type: Transaction (ID field: {id_field})")
        else:
            id_field = 'PART_NO'  # Default
            track_doc = False
            print(f"File Type: Unknown (using default ID field: {id_field})")
        
        # Detect changes
        changes = detector.detect_changes(dbf_file, id_field=id_field, track_doc_no=track_doc)
        
        # Display change summary
        print(f"\nChange Summary:")
        print(f"  🆕 New Records: {len(changes['new'])}")
        print(f"  🔄 Updated Records: {len(changes['updated'])}")
        print(f"  ❌ Deleted Records: {len(changes['deleted'])}")
        print(f"  ✓ Unchanged Records: {len(changes['unchanged'])}")
        
        # Show sample changes
        if changes['new']:
            print(f"\nSample NEW records (max 3):")
            for item in changes['new'][:3]:
                record = item['record']
                print(f"  - {item['record_id']}: {record}")
        
        if changes['updated']:
            print(f"\nSample UPDATED records (max 3):")
            for item in changes['updated'][:3]:
                record = item['record']
                print(f"  - {item['record_id']}: {record}")
        
        # Get records for synchronization
        sync_records = detector.get_changed_records_for_sync(changes)
        print(f"\n📤 Records ready for synchronization: {len(sync_records)}")
        
        # Simulate running again to show incremental behavior
        print(f"\n🔁 Running detection again (should show no changes)...")
        changes2 = detector.detect_changes(dbf_file, id_field=id_field, track_doc_no=track_doc)
        print(f"  Second run - New: {len(changes2['new'])}, Updated: {len(changes2['updated'])}")
    
    print("\n" + "=" * 80)
    print("✅ Step 2 Complete: Incremental Detection Implemented!")
    print("✅ State tracking file created: state.json")
    print("✅ Ready for Step 3: Data Transformation to CSV")
    print("=" * 80)


if __name__ == "__main__":
    try:
        demonstrate_incremental_detection()
    except KeyboardInterrupt:
        print("\n\n✓ Program terminated by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
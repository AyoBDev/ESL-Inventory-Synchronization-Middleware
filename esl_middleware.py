"""
ESL Inventory Synchronization Middleware - Cross-Platform Version
Works on both Windows and Mac/Linux
"""

import os
import sys
import time
import signal
import threading
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import json
import traceback
import platform

# Third-party imports
import schedule
from loguru import logger
from retrying import retry
import filelock

# Import our modules
from dbf_reader_with_memo import Config as DBFConfig, EnhancedDBFReader as DBFReader
from incremental_detector import IncrementalDetector, StateTracker
from data_transformer import FixedDataTransformer, Config as TransformerConfig


class ESLMiddleware:
    """Main middleware application with scheduling and error handling"""
    
    def __init__(self, config_file: str = "config.json"):
        """Initialize the middleware with configuration"""
        self.config = DBFConfig(config_file)
        self.running = False
        self.sync_in_progress = False
        self.last_sync_time = None
        self.sync_count = 0
        self.error_count = 0
        
        # Initialize components
        self.state_tracker = StateTracker(self.config.STATE_FILE)
        self.dbf_reader = DBFReader(self.config)
        self.detector = IncrementalDetector(self.config, self.state_tracker)
        self.transformer = FixedDataTransformer(TransformerConfig(config_file))
        
        # Setup logging
        self.setup_enhanced_logging()
        
        # Statistics tracking
        self.stats = {
            'total_syncs': 0,
            'successful_syncs': 0,
            'failed_syncs': 0,
            'records_processed': 0,
            'csv_files_created': 0,
            'last_error': None,
            'start_time': datetime.now()
        }
        
    def setup_enhanced_logging(self):
        """Setup comprehensive logging with rotation and multiple outputs"""
        log_dir = Path(self.config.LOG_DIR)
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # Remove default logger
        logger.remove()
        
        # Main log file
        main_log = log_dir / f"esl_middleware_{datetime.now():%Y%m%d}.log"
        logger.add(
            main_log,
            rotation="1 day",
            retention="30 days",
            level="INFO",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level:8} | {message}",
            backtrace=True,
            diagnose=True
        )
        
        # Error log file
        error_log = log_dir / "errors.log"
        logger.add(
            error_log,
            rotation="100 MB",
            retention="90 days",
            level="ERROR",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}\n{exception}",
            backtrace=True,
            diagnose=True
        )
        
        # Console output with color
        logger.add(
            sys.stdout,
            colorize=True,
            format="<green>{time:HH:mm:ss}</green> | <level>{level:8}</level> | <level>{message}</level>",
            level="INFO"
        )
        
        logger.info("=" * 80)
        logger.info("ESL INVENTORY SYNCHRONIZATION MIDDLEWARE STARTED")
        logger.info(f"Version: 1.0.0 (Cross-Platform)")
        logger.info(f"Platform: {platform.system()} {platform.release()}")
        logger.info(f"Python: {sys.version.split()[0]}")
        logger.info(f"Config: {self.config.config_file}")
        logger.info(f"Poll Interval: {self.config.POLL_INTERVAL} seconds")
        logger.info("=" * 80)
    
    @retry(
        wait_exponential_multiplier=1000,
        wait_exponential_max=10000,
        stop_max_attempt_number=3
    )
    def read_dbf_with_retry(self, file_path: Path, limit: Optional[int] = None) -> List[Dict]:
        """Read DBF file with retry logic for file locking issues"""
        try:
            # Try to acquire a file lock (non-blocking)
            lock_file = f"{file_path}.lock"
            lock_timeout = 10
            
            # Create lock with timeout
            lock = filelock.FileLock(lock_file, timeout=lock_timeout)
            
            try:
                with lock.acquire(timeout=lock_timeout):
                    return self.dbf_reader.read_dbf_file(file_path, limit)
            except filelock.Timeout:
                logger.warning(f"File locked: {file_path}. Retrying...")
                raise
                
        except Exception as e:
            logger.error(f"Error reading {file_path}: {e}")
            raise
    
    def process_single_file(self, dbf_file: Path) -> Dict[str, int]:
        """Process a single DBF file and return statistics"""
        stats = {
            'new_records': 0,
            'updated_records': 0,
            'deleted_records': 0,
            'csv_created': False,
            'error': None
        }
        
        try:
            logger.info(f"Processing: {dbf_file.name}")
            
            # Detect file type
            file_type = self.transformer.detect_file_type(dbf_file.name)
            id_field = 'DOC_NO' if file_type == 'TRANSACTION' else 'PART_NO'
            
            # Detect changes with retry logic
            changes = self.detector.detect_changes(
                dbf_file, 
                id_field=id_field,
                track_doc_no=(file_type == 'TRANSACTION')
            )
            
            # Update statistics
            stats['new_records'] = len(changes['new'])
            stats['updated_records'] = len(changes['updated'])
            stats['deleted_records'] = len(changes['deleted'])
            
            # Get records for synchronization
            sync_records = []
            for item in changes['new']:
                sync_records.append(item['record'])
            for item in changes['updated']:
                sync_records.append(item['record'])
            
            # Only create CSV if there are changes
            if sync_records:
                csv_path = self.transformer.transform_and_write_batch(
                    sync_records,
                    dbf_file.name,
                    file_type
                )
                
                if csv_path:
                    stats['csv_created'] = True
                    logger.success(f"✅ CSV created: {Path(csv_path).name} ({len(sync_records)} records)")
                else:
                    logger.warning(f"No CSV created for {dbf_file.name}")
            else:
                logger.info(f"No changes detected in {dbf_file.name}")
            
        except Exception as e:
            stats['error'] = str(e)
            logger.error(f"Error processing {dbf_file.name}: {e}")
            logger.debug(traceback.format_exc())
            
        return stats
    
    def sync_cycle(self):
        """Execute one synchronization cycle"""
        if self.sync_in_progress:
            logger.warning("Sync already in progress, skipping this cycle")
            return
        
        self.sync_in_progress = True
        cycle_start = datetime.now()
        
        try:
            logger.info(f"Starting sync cycle #{self.sync_count + 1}")
            
            # Find all DBF files
            dbf_files = self.dbf_reader.find_dbf_files()
            
            if not dbf_files:
                logger.warning(f"No DBF files found in {self.config.DBF_INPUT_DIR}")
                return
            
            # Process each file
            total_stats = {
                'files_processed': 0,
                'total_new': 0,
                'total_updated': 0,
                'total_deleted': 0,
                'csv_files_created': 0,
                'errors': []
            }
            
            for dbf_tuple in dbf_files:
                # If dbf_tuple is a tuple, extract the first element (Path), else use as is
                dbf_file = dbf_tuple[0] if isinstance(dbf_tuple, tuple) else dbf_tuple
                file_stats = self.process_single_file(dbf_file)
                
                total_stats['files_processed'] += 1
                total_stats['total_new'] += file_stats['new_records']
                total_stats['total_updated'] += file_stats['updated_records']
                total_stats['total_deleted'] += file_stats['deleted_records']
                
                if file_stats['csv_created']:
                    total_stats['csv_files_created'] += 1
                
                if file_stats['error']:
                    total_stats['errors'].append(f"{dbf_file.name}: {file_stats['error']}")
            
            # Update global statistics
            self.stats['total_syncs'] += 1
            self.stats['successful_syncs'] += 1
            self.stats['records_processed'] += (
                total_stats['total_new'] + 
                total_stats['total_updated']
            )
            self.stats['csv_files_created'] += total_stats['csv_files_created']
            
            # Log cycle summary
            cycle_duration = (datetime.now() - cycle_start).total_seconds()
            
            logger.info("=" * 60)
            logger.info(f"Sync Cycle #{self.sync_count + 1} Complete")
            logger.info(f"  Duration: {cycle_duration:.2f} seconds")
            logger.info(f"  Files Processed: {total_stats['files_processed']}")
            logger.info(f"  New Records: {total_stats['total_new']}")
            logger.info(f"  Updated Records: {total_stats['total_updated']}")
            logger.info(f"  Deleted Records: {total_stats['total_deleted']}")
            logger.info(f"  CSV Files Created: {total_stats['csv_files_created']}")
            
            if total_stats['errors']:
                logger.warning(f"  Errors: {len(total_stats['errors'])}")
                for error in total_stats['errors']:
                    logger.warning(f"    - {error}")
            
            logger.info("=" * 60)
            
            self.sync_count += 1
            self.last_sync_time = datetime.now()
            
        except Exception as e:
            self.stats['failed_syncs'] += 1
            self.stats['last_error'] = str(e)
            self.error_count += 1
            logger.error(f"Sync cycle failed: {e}")
            logger.debug(traceback.format_exc())
            
        finally:
            self.sync_in_progress = False
    
    def run_scheduler(self):
        """Run the scheduler in a separate thread"""
        while self.running:
            try:
                schedule.run_pending()
                time.sleep(1)
            except Exception as e:
                logger.error(f"Scheduler error: {e}")
                time.sleep(5)
    
    def display_status(self):
        """Display current status and statistics"""
        uptime = datetime.now() - self.stats['start_time']
        
        print("\n" + "=" * 80)
        print("ESL MIDDLEWARE STATUS")
        print("=" * 80)
        print(f"Platform: {platform.system()}")
        print(f"Status: {'RUNNING' if self.running else 'STOPPED'}")
        print(f"Uptime: {uptime}")
        print(f"Last Sync: {self.last_sync_time or 'Never'}")
        print(f"Total Syncs: {self.stats['total_syncs']}")
        print(f"Successful: {self.stats['successful_syncs']}")
        print(f"Failed: {self.stats['failed_syncs']}")
        print(f"Records Processed: {self.stats['records_processed']:,}")
        print(f"CSV Files Created: {self.stats['csv_files_created']}")
        print(f"Current Errors: {self.error_count}")
        
        if self.stats['last_error']:
            print(f"Last Error: {self.stats['last_error']}")
        
        print("=" * 80)
    
    def start(self):
        """Start the middleware"""
        logger.info("Starting ESL Middleware...")
        
        self.running = True
        
        # Run initial sync
        logger.info("Running initial synchronization...")
        self.sync_cycle()
        
        # Schedule periodic syncs
        schedule.every(self.config.POLL_INTERVAL).seconds.do(self.sync_cycle)
        
        # Schedule hourly status report
        schedule.every().hour.do(self.display_status)
        
        # Start scheduler thread
        scheduler_thread = threading.Thread(target=self.run_scheduler, daemon=True)
        scheduler_thread.start()
        
        logger.success(f"✅ Middleware started. Syncing every {self.config.POLL_INTERVAL} seconds")
        logger.info("Press Ctrl+C to stop")
        
        # Keep main thread alive
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()
    
    def stop(self):
        """Stop the middleware gracefully"""
        logger.info("Stopping ESL Middleware...")
        self.running = False
        
        # Wait for current sync to complete
        if self.sync_in_progress:
            logger.info("Waiting for current sync to complete...")
            timeout = 30
            start = time.time()
            while self.sync_in_progress and (time.time() - start) < timeout:
                time.sleep(0.5)
        
        # Display final status
        self.display_status()
        
        # Save state
        self.state_tracker.save_state()
        
        logger.success("✅ Middleware stopped gracefully")


def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info(f"Received signal {signum}. Shutting down...")
    if 'middleware' in globals():
        middleware.stop()
    sys.exit(0)


def main():
    """Main entry point"""
    global middleware
    print("\n" + "=" * 80)
    print("ESL INVENTORY SYNCHRONIZATION MIDDLEWARE")
    print("=" * 80)
    print("Version: 1.0.0 (Cross-Platform)")
    print(f"Platform: {platform.system()}")
    print("Press Ctrl+C to stop")
    print("=" * 80 + "\n")
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Check for command line arguments
    config_file = "config.json"
    if len(sys.argv) > 1:
        if sys.argv[1] in ['-h', '--help']:
            print("Usage: python3 esl_middleware.py [config_file]")
            print("  config_file: Path to configuration JSON file (default: config.json)")
            sys.exit(0)
        elif sys.argv[1] == '--test':
            print("Running in test mode (single sync cycle)...")
            middleware = ESLMiddleware(config_file)
            middleware.sync_cycle()
            middleware.display_status()
            sys.exit(0)
        else:
            config_file = sys.argv[1]
    
    # Check Python version
    if sys.version_info < (3, 7):
        print(f"⚠️  Python 3.7+ required. Current: {sys.version}")
        sys.exit(1)
    
    # Start middleware
    try:
        middleware = ESLMiddleware(config_file)
        middleware.start()
    except FileNotFoundError as e:
        print(f"\n❌ Error: {e}")
        print("\nMake sure you have created all required files:")
        print("  - dbf_reader.py")
        print("  - incremental_detector.py")
        print("  - data_transformer.py")
        print("  - config.json")
        sys.exit(1)
    except ImportError as e:
        print(f"\n❌ Import Error: {e}")
        print("\nInstall missing modules:")
        print("  pip3 install -r requirements.txt")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Failed to start middleware: {e}")
        logger.debug(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
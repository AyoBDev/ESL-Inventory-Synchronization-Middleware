"""
ESL Middleware Windows Service
Allows the middleware to run as a Windows Service
"""

import os
import sys
import time
import win32serviceutil # type: ignore
import win32service # type: ignore
import win32event # type: ignore
import servicemanager
import socket
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from esl_middleware import ESLMiddleware


class ESLMiddlewareService(win32serviceutil.ServiceFramework):
    """Windows Service wrapper for ESL Middleware"""
    
    _svc_name_ = "ESLMiddleware"
    _svc_display_name_ = "ESL Inventory Synchronization Middleware"
    _svc_description_ = "Synchronizes inventory data from R-MPOS DBF files to ESL CSV files"
    
    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.stop_event = win32event.CreateEvent(None, 0, 0, None)
        self.middleware = None
        socket.setdefaulttimeout(60)
        
    def SvcStop(self):
        """Stop the service"""
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.stop_event)
        
        # Stop the middleware
        if self.middleware:
            self.middleware.stop()
        
    def SvcDoRun(self):
        """Run the service"""
        servicemanager.LogMsg(
            servicemanager.EVENTLOG_INFORMATION_TYPE,
            servicemanager.PYS_SERVICE_STARTED,
            (self._svc_name_, '')
        )
        
        self.main()
        
    def main(self):
        """Main service loop"""
        try:
            # Get the service installation directory
            service_dir = Path(sys.executable).parent
            os.chdir(service_dir)
            
            # Initialize and start middleware
            config_file = service_dir / "config.json"
            self.middleware = ESLMiddleware(str(config_file))
            
            # Run middleware in a thread
            import threading
            middleware_thread = threading.Thread(target=self.middleware.start)
            middleware_thread.daemon = True
            middleware_thread.start()
            
            # Wait for stop signal
            win32event.WaitForSingleObject(self.stop_event, win32event.INFINITE)
            
        except Exception as e:
            servicemanager.LogErrorMsg(f"Service failed: {e}")
            raise


def install_service():
    """Install the Windows service"""
    print("Installing ESL Middleware Service...")
    win32serviceutil.InstallService(
        ESLMiddlewareService,
        ESLMiddlewareService._svc_name_,
        ESLMiddlewareService._svc_display_name_,
        startType=win32service.SERVICE_AUTO_START,
        description=ESLMiddlewareService._svc_description_
    )
    print(f"✅ Service '{ESLMiddlewareService._svc_display_name_}' installed successfully")
    print("To start the service, run: python windows_service.py start")


def remove_service():
    """Remove the Windows service"""
    print("Removing ESL Middleware Service...")
    win32serviceutil.RemoveService(ESLMiddlewareService._svc_name_)
    print(f"✅ Service '{ESLMiddlewareService._svc_display_name_}' removed successfully")


if __name__ == '__main__':
    if len(sys.argv) == 1:
        # Running as a service
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(ESLMiddlewareService)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        # Handle command line arguments
        if sys.argv[1] == 'install':
            install_service()
        elif sys.argv[1] == 'remove':
            remove_service()
        else:
            # Standard service operations (start, stop, restart, etc.)
            win32serviceutil.HandleCommandLine(ESLMiddlewareService)
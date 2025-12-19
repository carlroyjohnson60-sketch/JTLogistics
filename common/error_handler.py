import os
import sys
import traceback
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any
from .email_notifier import EmailNotifier


class ErrorLogger:
    """Handle error logging, persistence, and notifications."""

    def __init__(self, log_folder: str = None, app_name: str = "PythonUpdate"):
        
        self.app_name = app_name
        self.log_folder = log_folder or os.path.join(os.getcwd(), "logs")
        self.error_folder = os.path.join(self.log_folder, "errors")
        self.warning_folder = os.path.join(self.log_folder, "warnings")
        self.info_folder = os.path.join(self.log_folder, "info")
        
        # Create folder structure
        self._ensure_directories()
        
        # Setup logging
        self.logger = self._setup_logger()
        self.errors = []
        self.warnings = []

    def _ensure_directories(self):
        """Create all required log directories."""
        for folder in [self.log_folder, self.error_folder, self.warning_folder, self.info_folder]:
            Path(folder).mkdir(parents=True, exist_ok=True)

    def _setup_logger(self) -> logging.Logger:
        """Setup main application logger."""
        logger = logging.getLogger(self.app_name)
        logger.setLevel(logging.DEBUG)
        
        # Clear existing handlers
        logger.handlers = []
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        
        # File handler for all logs
        main_log_file = os.path.join(self.log_folder, f"{self.app_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
        file_handler = logging.FileHandler(main_log_file)
        file_handler.setLevel(logging.DEBUG)
        
        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)
        
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)
        
        return logger

    def log_error(self, error_msg: str, context: Dict[str, Any] = None, exc_info: bool = False):
        
        context = context or {}
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Create error record
        error_record = {
            'timestamp': timestamp,
            'message': error_msg,
            'context': context,
            'traceback': traceback.format_exc() if exc_info else None
        }
        
        self.errors.append(error_record)
        
        # Log to main logger
        if exc_info:
            self.logger.error(f"{error_msg} | Context: {context}", exc_info=True)
        else:
            self.logger.error(f"{error_msg} | Context: {context}")
        
        # Write to dedicated error file
        self._write_error_file(error_record)

    def log_warning(self, warning_msg: str, context: Dict[str, Any] = None):
        
        context = context or {}
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        warning_record = {
            'timestamp': timestamp,
            'message': warning_msg,
            'context': context
        }
        
        self.warnings.append(warning_record)
        self.logger.warning(f"{warning_msg} | Context: {context}")
        self._write_warning_file(warning_record)

    def log_info(self, info_msg: str, context: Dict[str, Any] = None):
       
        context = context or {}
        self.logger.info(f"{info_msg} | Context: {context}")

    def _write_error_file(self, error_record: Dict[str, Any]):
        """Write error record to dedicated error log file."""
        timestamp = error_record['timestamp'].replace(':', '_').replace(' ', '_')
        error_file = os.path.join(
            self.error_folder,
            f"error_{timestamp}.log"
        )
        
        try:
            with open(error_file, 'w', encoding='utf-8') as f:
                f.write(f"Timestamp: {error_record['timestamp']}\n")
                f.write(f"Message: {error_record['message']}\n")
                f.write(f"Context: {error_record['context']}\n")
                if error_record.get('traceback'):
                    f.write(f"\nTraceback:\n{error_record['traceback']}\n")
        except Exception as e:
            self.logger.error(f"Failed to write error file: {e}")

    def _write_warning_file(self, warning_record: Dict[str, Any]):
        """Write warning record to dedicated warning log file."""
        timestamp = warning_record['timestamp'].replace(':', '_').replace(' ', '_')
        warning_file = os.path.join(
            self.warning_folder,
            f"warning_{timestamp}.log"
        )
        
        try:
            with open(warning_file, 'w', encoding='utf-8') as f:
                f.write(f"Timestamp: {warning_record['timestamp']}\n")
                f.write(f"Message: {warning_record['message']}\n")
                f.write(f"Context: {warning_record['context']}\n")
        except Exception as e:
            self.logger.error(f"Failed to write warning file: {e}")

    def get_error_count(self) -> int:
        """Get total number of errors logged."""
        return len(self.errors)

    def get_warning_count(self) -> int:
        """Get total number of warnings logged."""
        return len(self.warnings)

    def get_summary(self) -> Dict[str, Any]:
        """Get summary of errors and warnings."""
        return {
            'error_count': self.get_error_count(),
            'warning_count': self.get_warning_count(),
            'errors': self.errors,
            'warnings': self.warnings
        }

    def has_errors(self) -> bool:
        """Check if any errors have been logged."""
        return len(self.errors) > 0


class ErrorHandler:
    """Decorator and context manager for error handling."""

    def __init__(self, error_logger: ErrorLogger, email_notifier=None):
        
        self.error_logger = error_logger
        self.email_notifier = email_notifier

    def handle_function(self, func_name: str, context: Dict[str, Any] = None):
        """Decorator for handling function errors."""
        def decorator(func):
            def wrapper(*args, **kwargs):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    error_context = context or {}
                    error_context['function'] = func_name
                    error_context['args'] = str(args)[:200]  # Limit size
                    error_context['kwargs'] = str(kwargs)[:200]
                    
                    self.error_logger.log_error(
                        f"Error in {func_name}: {str(e)}",
                        context=error_context,
                        exc_info=True
                    )
                    
                    # Send email notification if configured
                    if self.email_notifier:
                        try:
                            self.email_notifier.send(
                                subject=f"ERROR: {func_name}",
                                body=f"An error occurred in {func_name}:\n\n{str(e)}\n\nContext: {error_context}"
                            )
                        except Exception as email_err:
                            self.error_logger.log_error(
                                f"Failed to send error email: {str(email_err)}",
                                exc_info=True
                            )
                    
                    raise
            return wrapper
        return decorator

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - handle exceptions."""
        if exc_type is not None:
            self.error_logger.log_error(
                f"Exception: {exc_val}",
                context={'exception_type': exc_type.__name__},
                exc_info=True
            )
            
            # Send email notification if configured
            if self.email_notifier:
                try:
                    self.email_notifier.send(
                        subject=f"ERROR: {exc_type.__name__}",
                        body=f"An exception occurred:\n\n{traceback.format_exc()}"
                    )
                except Exception as email_err:
                    self.error_logger.log_error(
                        f"Failed to send error email: {str(email_err)}",
                        exc_info=True
                    )
        
        return False  # Don't suppress exceptions

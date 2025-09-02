"""Structured logging utilities with security-aware data sanitization"""

import logging
import json
from typing import Any, Dict, Optional
from datetime import datetime


class StructuredLogger:
    """Structured logger with automatic sensitive data filtering"""
    
    SENSITIVE_KEYS = {
        'token', 'password', 'secret', 'key', 'authorization', 'auth',
        'cronofy_access_token', 'algolia_api_key', 'database_url'
    }
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
    
    def _sanitize_data(self, data: Any) -> Any:
        """Recursively sanitize sensitive data from logging payload"""
        if isinstance(data, dict):
            sanitized = {}
            for key, value in data.items():
                key_lower = key.lower()
                if any(sensitive in key_lower for sensitive in self.SENSITIVE_KEYS):
                    # Replace with masked value but preserve length info
                    if isinstance(value, str):
                        sanitized[key] = f"[MASKED:{len(value)}]"
                    else:
                        sanitized[key] = "[MASKED]"
                else:
                    sanitized[key] = self._sanitize_data(value)
            return sanitized
        elif isinstance(data, (list, tuple)):
            return [self._sanitize_data(item) for item in data]
        else:
            return data
    
    def _create_log_entry(self, level: str, message: str, **kwargs) -> Dict[str, Any]:
        """Create structured log entry"""
        entry = {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'level': level,
            'message': message,
        }
        
        # Add sanitized context data
        if kwargs:
            entry['context'] = self._sanitize_data(kwargs)
        
        return entry
    
    def info(self, message: str, **kwargs):
        """Log info level with structured data"""
        entry = self._create_log_entry('INFO', message, **kwargs)
        self.logger.info(json.dumps(entry))
    
    def warning(self, message: str, **kwargs):
        """Log warning level with structured data"""
        entry = self._create_log_entry('WARNING', message, **kwargs)
        self.logger.warning(json.dumps(entry))
    
    def error(self, message: str, **kwargs):
        """Log error level with structured data"""
        entry = self._create_log_entry('ERROR', message, **kwargs)
        self.logger.error(json.dumps(entry))
    
    def debug(self, message: str, **kwargs):
        """Log debug level with structured data"""
        entry = self._create_log_entry('DEBUG', message, **kwargs)
        self.logger.debug(json.dumps(entry))


def get_structured_logger(name: str) -> StructuredLogger:
    """Get a structured logger instance"""
    return StructuredLogger(logging.getLogger(name))
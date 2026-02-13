import logging
import json
import sys
from datetime import datetime
from typing import Any, Dict

class JSONFormatter(logging.Formatter):
    """
    Formatter that outputs JSON strings for structured logging.
    """
    def format(self, record: logging.LogRecord) -> str:
        log_record = {
            "timestamp": datetime.now().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        
        # Merge extra fields if they exist in record.__dict__
        # This allows logger.info("msg", extra={"token_id": 123})
        if hasattr(record, "token_id"):
            log_record["token_id"] = record.token_id
            
        if hasattr(record, "event"):
            log_record["event"] = record.event
            
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)
            
        return json.dumps(log_record)

def get_logger(name: str, level=logging.INFO) -> logging.Logger:
    """
    Returns a logger configured with JSON formatting.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Check if handler already exists to avoid duplicates
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JSONFormatter())
        logger.addHandler(handler)
        
    return logger

def log_event(logger: logging.Logger, event: str, data: Dict[str, Any], level=logging.INFO):
    """
    Helper to log a structured event.
    data is merged into the JSON payload.
    """
    payload = {
        "event": event,
        **data
    }
    # We pass the payload as a string message, but ideally we want fields.
    # Because our JSONFormatter is simple, let's just dump the whole thing as message
    # OR we can inject fields.
    # Proper structured logging usually replaces the message.
    
    # Strategy: Dump full payload as the "message" part, or use extra?
    # Let's use extra for cleanliness if the formatter supports it, 
    # but the simple formatter above just merges specific fields.
    
    # Better approach for this system:
    # Just log the dictionary as a string, and let the formatter wrap it?
    # No, we want distinct keys.
    
    # Let's just output the json directly via the logger
    # logger.info(json.dumps(payload))
    # But then we get double JSON if valid formatter.
    
    # Revised Strategy:
    # Just use the logger to print the JSON string if it's the root logger.
    # Or use the custom formatter we made.
    
    # If we use the custom formatter, we should pass dicts to it?
    # Standard python logging expects string messages.
    
    # Let's keep it simple:
    # Logger logs valid JSON strings.
    logger.log(level, json.dumps(payload))

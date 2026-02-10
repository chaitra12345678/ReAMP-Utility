import logging
import json
from datetime import datetime
from pathlib import Path

class JsonFormatter(logging.Formatter):
    """JSON log formatter for structured logging."""
    
    def format(self, record):
        log_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
        }
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_data)


class UtilityLogger:
    """Standard logger for ReAMP utilities."""
    
    def __init__(self, utility_name, log_dir="logs"):
        self.utility_name = utility_name
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
        
        self.logger = logging.getLogger(utility_name)
        self.logger.setLevel(logging.DEBUG)
        
        # File handler with JSON format
        log_file = self.log_dir / f"{utility_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(JsonFormatter())
        self.logger.addHandler(file_handler)
        
        # Console handler with simple format
        console_handler = logging.StreamHandler()
        console_format = logging.Formatter("[%(levelname)s] %(message)s")
        console_handler.setFormatter(console_format)
        self.logger.addHandler(console_handler)
    
    def info(self, message):
        self.logger.info(message)
    
    def error(self, message):
        self.logger.error(message)
    
    def warning(self, message):
        self.logger.warning(message)
    
    def debug(self, message):
        self.logger.debug(message)


def setup_logger(name: str, log_level: str = 'INFO'):
    """Create or retrieve a stdlib logger configured with stream handler.

    This lightweight helper is used by scheduler and other modules that prefer
    simple logging configuration instead of UtilityLogger.
    """
    lvl = getattr(logging, log_level.upper(), logging.INFO)
    logger = logging.getLogger(name)
    logger.setLevel(lvl)
    # Avoid adding duplicate handlers on repeated calls
    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        ch = logging.StreamHandler()
        fmt = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        ch.setFormatter(fmt)
        logger.addHandler(ch)
    return logger

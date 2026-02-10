import re
import os
from pathlib import Path
from typing import Union, List


class Validators:
    """Common validation utilities for ReAMP utilities."""
    
    @staticmethod
    def validate_unc_path(path: str) -> dict:
        """Validate UNC path format and accessibility."""
        result = {
            "valid": False,
            "path": path,
            "accessible": False,
            "reason": ""
        }
        
        # Check UNC format (\\server\share)
        unc_pattern = r'^\\\\[a-zA-Z0-9\-_.]+\\[a-zA-Z0-9\-_$]+.*'
        if not re.match(unc_pattern, path):
            result["reason"] = "Invalid UNC path format"
            return result
        
        result["valid"] = True
        
        # Check accessibility
        try:
            if os.path.exists(path):
                result["accessible"] = True
            else:
                result["reason"] = "Path does not exist or is not accessible"
        except Exception as e:
            result["reason"] = str(e)
        
        return result
    
    @staticmethod
    def validate_email(email: str) -> bool:
        """Validate email format."""
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return re.match(pattern, email) is not None
    
    @staticmethod
    def validate_sid(sid: str) -> bool:
        """Validate Windows SID format."""
        pattern = r'^S-1-[0-9]+-(\d+-)*\d+$'
        return re.match(pattern, sid) is not None
    
    @staticmethod
    def validate_required_fields(data: dict, required_fields: List[str]) -> dict:
        """Validate that required fields are present and non-empty."""
        missing = []
        for field in required_fields:
            if field not in data or not data[field]:
                missing.append(field)
        
        return {
            "valid": len(missing) == 0,
            "missing_fields": missing
        }
    
    @staticmethod
    def validate_file_path(file_path: str) -> dict:
        """Validate file path and accessibility."""
        result = {
            "valid": False,
            "path": file_path,
            "exists": False,
            "readable": False,
            "reason": ""
        }
        
        try:
            path = Path(file_path)
            result["exists"] = path.exists()
            
            if result["exists"]:
                result["readable"] = os.access(file_path, os.R_OK)
                result["valid"] = result["readable"]
            else:
                result["reason"] = "File does not exist"
        except Exception as e:
            result["reason"] = str(e)
        
        return result

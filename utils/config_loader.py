import json
import yaml
from pathlib import Path
from typing import Any, Dict, Optional


class ConfigLoader:
    """Load configuration from JSON or YAML files."""
    
    @staticmethod
    def load_json(file_path: str) -> Dict[str, Any]:
        """Load JSON configuration file."""
        with open(file_path, 'r') as f:
            return json.load(f)
    
    @staticmethod
    def load_yaml(file_path: str) -> Dict[str, Any]:
        """Load YAML configuration file."""
        with open(file_path, 'r') as f:
            return yaml.safe_load(f)
    
    @staticmethod
    def load(file_path: str) -> Dict[str, Any]:
        """Auto-detect and load config file based on extension."""
        path = Path(file_path)
        if path.suffix.lower() in ['.json']:
            return ConfigLoader.load_json(file_path)
        elif path.suffix.lower() in ['.yaml', '.yml']:
            return ConfigLoader.load_yaml(file_path)
        else:
            raise ValueError(f"Unsupported config format: {path.suffix}")
    
    @staticmethod
    def save_json(file_path: str, data: Dict[str, Any]) -> None:
        """Save configuration to JSON file."""
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2)
    
    @staticmethod
    def merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively merge override config into base config."""
        result = base.copy()
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = ConfigLoader.merge(result[key], value)
            else:
                result[key] = value
        return result

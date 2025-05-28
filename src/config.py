import yaml
import logging
import os
from typing import Dict


class Config:
    def __init__(self, config_file_name: str = "config.yaml"):
        # Construct path relative to this config.py file
        base_dir = os.path.dirname(os.path.abspath(__file__))
        config_file_path = os.path.join(base_dir, config_file_name) # This will point to src/config.yaml
        try:
            with open(config_file_path, 'r') as f:
                config_data = yaml.safe_load(f)
        except FileNotFoundError:
            logging.error(f"Configuration file {config_file_path} not found") # Log the full path
            raise
        self.proxy_config = config_data.get("proxy", {})
        self.dialect_configs = config_data.get("databases", {})
        self.source_dialect = self.proxy_config.get("source_dialect", "mysql")
        self.port = int(self.proxy_config.get("port", 3306))

    def get_target_dialect(self) -> str:
        # Assuming PostgreSQL is still the primary target for now
        return "postgresql"

    def get_dialect_config(self, dialect: str) -> Dict:
        return self.dialect_configs.get(dialect, {})

CONFIG = Config()

import yaml
import logging
from typing import Dict

class Config:
    def __init__(self, config_file: str = "config.yaml"):
        try:
            with open(config_file, 'r') as f:
                config_data = yaml.safe_load(f)
        except FileNotFoundError:
            logging.error(f"Configuration file {config_file} not found")
            raise
        self.proxy_config = config_data.get("proxy", {})
        self.dialect_configs = config_data.get("databases", {})
        self.source_dialect = self.proxy_config.get("source_dialect", "mysql")
        self.port = int(self.proxy_config.get("port", 3306))

    def get_target_dialect(self) -> str:
        return "postgresql"

    def get_dialect_config(self, dialect: str) -> Dict:
        return self.dialect_configs.get(dialect, {})

CONFIG = Config()

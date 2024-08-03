from __future__ import annotations

import logging
from pathlib import Path
from typing import Tuple

import dotenv
import yaml

from utils.logging_config import setup_logging


def load_config(config_path: Path) -> dict:
    """Load and return the project configuration."""
    with open(config_path, 'r', encoding='utf-8') as file:
        return yaml.safe_load(file)


def initialize_project(
        config_filename: str = 'config/config.yaml',
        env_filename: str = '.env',
        log_filename: str = None) -> Tuple[Path, dict]:
    """Initialize the project environment, load configuration, and set up logging."""
    project_dir = Path(__file__).resolve().parents[1]

    # Load environment variables
    dotenv.load_dotenv(project_dir / env_filename)

    # Load configuration
    config_path = project_dir / config_filename
    project_config = load_config(config_path)

    # Set up logging
    log_filename = log_filename or f'{Path(__file__).stem}.log'
    setup_logging('DataPipeline', project_dir, log_filename, project_config)

    return project_dir, project_config


def get_logger(name: str) -> logging.Logger:
    """Get a logger with the given name."""
    return logging.getLogger(name)


def setup_project():
    """Set up project environment, configuration, and logging."""
    project_dir, project_config = initialize_project()
    logging.getLogger().setLevel(logging.DEBUG)
    return project_dir, project_config


if __name__ == "__main__":
    project_dir, project_config = setup_project()
    logging.info("Project setup completed successfully.")

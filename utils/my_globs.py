from __future__ import annotations

from pathlib import Path

import yaml

project_dir = Path(__file__).resolve().parents[1]

config_filename = 'config.yaml'
config_path = project_dir / config_filename
with open(config_path, 'r', encoding='utf-8') as file:
    project_config = yaml.safe_load(file)

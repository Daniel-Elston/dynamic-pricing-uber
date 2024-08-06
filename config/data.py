from __future__ import annotations

import logging
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from pprint import pformat
from typing import Dict

import yaml


@dataclass
class DataPaths:
    config_path: Path = Path('config/paths.yaml')
    paths: Dict[str, Path] = field(default_factory=dict)

    def __post_init__(self):
        with open(self.config_path, 'r') as f:
            config = yaml.safe_load(f)
        self.paths = {k: Path(v) for k, v in config['data_state_paths'].items()}

    def get_path(self, key: str) -> Path:
        return self.paths[key]

    def validate_paths(self):
        for name, path in self.paths.items():
            if not path.exists():
                logging.warning(f"Path {name} does not exist: {path}. Check paths.yaml")


@dataclass
class DataConfig:
    overwrite: bool = True
    save_fig: bool = True
    window_select: int = 6
    ma_windows: list = field(default_factory=lambda: [1, 3, 6, 12, 24])
    lag_windows: list = field(default_factory=lambda: [1, 3, 6, 12, 24])


@dataclass
class DataState:
    paths: DataPaths
    config: DataConfig

    def __post_init__(self):
        self.paths.validate_paths()
        logging.debug(f"Initialized DataState with paths:\n{pformat(self.paths)}")

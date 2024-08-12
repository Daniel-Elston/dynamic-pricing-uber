from __future__ import annotations

import logging
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import Dict
from typing import Optional
from typing import Union

import yaml


@dataclass
class PathsConfig:
    config_path: Path = Path('config/paths.yaml')
    paths: Dict[str, Path] = field(default_factory=dict)

    def __post_init__(self):
        with open(self.config_path, 'r') as f:
            config = yaml.safe_load(f)
        self.paths = {k: Path(v) for k, v in config['data_state_paths'].items()}

    def get_path(self, key: Optional[Union[str, Path]]) -> Optional[Path]:
        if key is None:
            return None
        if isinstance(key, Path):
            return key
        return self.paths.get(key)

    def validate_paths(self):
        for name, path in self.paths.items():
            if not path.exists():
                logging.warning(f"Path {name} does not exist: {path}")

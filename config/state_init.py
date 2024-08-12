from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field

from config.data import DataConfig
from config.model import ModelConfig
from config.paths import PathsConfig


@dataclass
class StateManager:
    paths: PathsConfig = field(default_factory=PathsConfig)
    data_config: DataConfig = field(default_factory=DataConfig)
    model_config: ModelConfig = field(default_factory=ModelConfig)

    def __post_init__(self):
        self.validate_paths()

    def validate_paths(self):
        self.paths.validate_paths()

from __future__ import annotations

from pathlib import Path
from typing import Callable
from typing import List
from typing import Union

from config.data import DataConfig
from config.data import DataState
from utils.file_access import FileAccess
from utils.logging_utils import log_step


class Running:
    def __init__(self, data_state: DataState, data_config: DataConfig):
        self.ds = data_state
        self.dc = data_config

    def run_step(self, step: Callable, load_path: str, save_paths: Union[str, List[str], None] = None):
        load_path = self.ds.paths.get_path(load_path)

        if save_paths is not None:
            save_paths = self.ds.paths.get_path(save_paths) if isinstance(save_paths, str) else [self.ds.paths.get_path(path) for path in save_paths]

        with FileAccess.load_file(load_path) as df:
            logged_step = log_step(load_path, save_paths)(step)
            result = logged_step(df, save_paths if isinstance(save_paths, Path) else save_paths)

            if isinstance(save_paths, Path):
                FileAccess.save_file(result, save_paths, self.dc.overwrite)

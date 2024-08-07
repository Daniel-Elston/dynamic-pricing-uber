from __future__ import annotations

from pathlib import Path
from typing import Callable
from typing import List
from typing import Optional
from typing import Union

import pandas as pd

from config.data import DataConfig
from config.data import DataState
from utils.file_access import FileAccess
from utils.logging_utils import log_step


class Running:
    def __init__(self, data_state: DataState, data_config: DataConfig):
        self.ds = data_state
        self.dc = data_config

    def run_main_step(
            self, step: Callable,
            load_path: Optional[Union[str, List[str], Path]] = None,
            save_paths: Optional[Union[str, List[str], Path]] = None,
            args: Optional[Union[dict, None]] = None,
            kwargs: Optional[Union[dict, None]] = None) -> pd.DataFrame:
        """Pipeline runner for top-level main.py."""
        if load_path:
            load_path = self.ds.paths.get_path(load_path)
        if save_paths and isinstance(save_paths, str):
            save_paths = self.ds.paths.get_path(save_paths)
        if save_paths and isinstance(save_paths, list):
            save_paths = [self.ds.paths.get_path(path) for path in save_paths]
        return step(**args) if args is not None else step()

    def run_parent_step(
            self, step: Callable,
            load_path: Optional[Union[str, List[str], Path]] = None,
            save_paths: Optional[Union[str, List[str], Path]] = None,
            df: Optional[Union[pd.DataFrame]] = None):
        """Pipeline runner for parent pipelines scripts (src/pipelines/*)"""
        if load_path is None:
            self._parent_save_helper(step, df, load_path, save_paths)

        else:
            load_path = self.ds.paths.get_path(load_path)
            with FileAccess.load_file(load_path) as df:
                self._parent_save_helper(step, df, load_path, save_paths)

    def _parent_save_helper(self, step, df, load_path, save_paths):
        if save_paths is not None:
            if isinstance(save_paths, str):
                save_paths = self.ds.paths.get_path(save_paths)
                logged_step = log_step(load_path, save_paths)(step)
                result = logged_step(df)
                FileAccess.save_file(result, save_paths, self.dc.overwrite)
            if isinstance(save_paths, list):
                save_paths = [self.ds.paths.get_path(path) for path in save_paths]
                for path in save_paths:
                    logged_step = log_step(load_path, save_paths)(step)
                    result = logged_step(df)
                    FileAccess.save_file(result, path, self.dc.overwrite)
        else:
            logged_step = log_step(load_path, save_paths)(step)
            result = logged_step(df)
            return result

    def run_child_step(
            self, step: Callable,
            df: pd.DataFrame,
            args: Optional[Union[dict, None]] = None,
            kwargs: Optional[Union[dict, None]] = None) -> pd.DataFrame:
        """Pipeline runner for child pipelines scripts (lowest level scripts)"""
        return step(df, **args) if args is not None else step(df)

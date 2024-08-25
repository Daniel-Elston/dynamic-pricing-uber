from __future__ import annotations

import logging
from pathlib import Path
from typing import Callable
from typing import List
from typing import Optional
from typing import Union

import pandas as pd

from config.state_init import StateManager
from utils.file_access import FileAccess
from utils.logging_utils import log_step


class TaskExecutor:
    def __init__(self, state: StateManager):
        self.data_config = state.data_config
        self.paths = state.paths

    def run_main_step(
            self, step: Callable,
            load_path: Optional[Union[str, List[str], Path]] = None,
            save_paths: Optional[Union[str, List[str], Path]] = None,
            args: Optional[Union[dict, None]] = None,
            kwargs: Optional[Union[dict, None]] = None) -> pd.DataFrame:
        """Pipeline runner for top-level main.py."""
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
            load_path = self.paths.get_path(load_path)
            with FileAccess.load_file(load_path) as df:
                self._parent_save_helper(step, df, load_path, save_paths)

    def _parent_save_helper(self, step, df, load_path, save_paths):
        if save_paths is not None:
            if isinstance(save_paths, str):
                save_paths = self.paths.get_path(save_paths)
                logged_step = log_step(load_path, save_paths)(step)
                result = logged_step(df)
                FileAccess.save_file(result, save_paths, self.data_config.overwrite)
            if isinstance(save_paths, list):
                save_paths = [self.paths.get_path(path) for path in save_paths]
                for path in save_paths:
                    logged_step = log_step(load_path, save_paths)(step)
                    result = logged_step(df)
                    FileAccess.save_file(result, path, self.data_config.overwrite)
        else:
            logged_step = log_step(load_path, save_paths)(step)
            result = logged_step(df)
            return result

    @staticmethod
    def run_child_step(
            step: Callable,
            df: pd.DataFrame,
            args: Optional[Union[dict, None]] = None,
            kwargs: Optional[Union[dict, None]] = None) -> pd.DataFrame:
        """Pipeline runner for child pipelines scripts (lowest level scripts)"""
        return step(df, **args) if args is not None else step(df)

    def _execute_steps(self, steps, stage=None):
        if stage == "main":
            for step, load_path, save_paths in steps:
                logging.info(
                    f"INITIATING {step.__self__.__class__.__name__} with:\n"
                    f"    Input_path: {self.paths.get_path(load_path)}\n"
                    f"    Output_paths: {self.paths.get_path(save_paths)}\n"
                )
                self.run_main_step(step, load_path, save_paths)
                logging.info(f"{step.__self__.__class__.__name__} completed SUCCESSFULLY.\n")
        if stage == "parent":
            for step, load_path, save_paths in steps:
                self.run_parent_step(step, load_path, save_paths)

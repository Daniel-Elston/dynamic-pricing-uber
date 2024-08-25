from __future__ import annotations

import logging

from config.state_init import StateManager
from src.pipelines.data_pipeline import DataPipeline
from utils.execution import TaskExecutor
from utils.project_setup import setup_project


class MainPipeline:
    def __init__(self, state: StateManager, exe: TaskExecutor):
        self.state = state
        self.exe = exe

    def run(self):
        logging.info(
            f"INITIATING {self.__class__.__name__} from top-level script: ``{__file__.split('/')[-1]}``...\n"
        )
        steps = [
            (DataPipeline(state, exe).main, 'raw', 'result'),
        ]
        self.exe._execute_steps(steps, stage="main")
        logging.info(
            f'Completed {self.__class__.__name__} from top-level script: ``{__file__.split("/")[-1]}`` SUCCESSFULLY.\n'
        )


if __name__ == '__main__':
    project_dir, project_config, state = setup_project()
    exe = TaskExecutor(state)

    try:
        MainPipeline(state, exe).run()
    except Exception as e:
        logging.error(f'Pipeline terminated due to unexpected error: {e}', exc_info=True)

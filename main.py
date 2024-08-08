from __future__ import annotations

import logging

from config.data import DataConfig
from config.data import DataPaths
from config.data import DataState
from config.model import ModelConfig
from src.pipelines.data_pipeline import DataPipeline
from utils.execution import TaskExecutor
from utils.project_setup import setup_project


class MainPipeline:
    def __init__(self, exe: TaskExecutor, data_state: DataState, model_config: ModelConfig):
        self.exe = exe
        self.ds = data_state
        self.dc = self.ds.config
        self.mc = model_config

    def run(self):
        steps = [
            (DataPipeline(exe, ds, mc), 'raw', 'result'),
        ]
        for step, load_path, save_paths in steps:
            logging.info(
                f'INITIATING {step.__class__.__name__} with:\n\
                    Input_path: ``{self.ds.paths.get_path(load_path)}``\n\
                    Output_paths: ``{self.ds.paths.get_path(save_paths)}``\n')
            self.exe.run_main_step(step.main, load_path, save_paths)
            logging.info(f'{step.__class__.__name__} completed SUCCESSFULLY.\n')


if __name__ == '__main__':
    project_dir, project_config = setup_project()

    dc, dp = DataConfig(), DataPaths()
    ds = DataState(dp, dc)
    mc = ModelConfig()
    exe = TaskExecutor(ds)

    logging.info("INITIATING main.py MainPipeline...\n")
    try:
        MainPipeline(exe, ds, mc).run()
    except Exception as e:
        logging.error(f'Pipeline terminated due to unexpected error: {e}', exc_info=True)

from __future__ import annotations

import logging

from config.data import DataConfig
from config.data import DataPaths
from config.data import DataState
from config.model import ModelConfig
from src.pipelines.data_pipeline import DataPipeline
from utils.project_setup import setup_project
from utils.running import Running


class MainPipeline:
    def __init__(self, data_state: DataState, data_config: DataConfig, model_config: ModelConfig):
        self.ds = data_state
        self.dc = data_config
        self.mc = model_config
        self.runner = Running(self.ds, self.dc)

    def run(self):
        steps = [
            (DataPipeline(ds, dc, mc), 'raw', 'result'),
        ]
        for step, load_path, save_paths in steps:
            logging.info(
                f'INITIATING {step.__class__.__name__} with:\n\
                    Input_path: ``{self.ds.paths.get_path(load_path)}\n\
                    Output_paths: ``{self.ds.paths.get_path(save_paths)}``\n')
            self.runner.run_main_step(step.main, load_path, save_paths)
            logging.info(f'{step.__class__.__name__} completed SUCCESSFULLY.\n')


if __name__ == '__main__':
    project_dir, project_config = setup_project()

    dc, dp = DataConfig(), DataPaths()
    ds = DataState(dp, dc)
    mc = ModelConfig()
    runner = Running(ds, dc)

    logging.info("INITIATING main.py MainPipeline...\n")
    try:
        MainPipeline(ds, dc, mc).run()
    except Exception as e:
        logging.error(f'Pipeline terminated due to unexpected error: {e}', exc_info=False)

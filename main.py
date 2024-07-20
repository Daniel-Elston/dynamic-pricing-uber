from __future__ import annotations

import logging
from pathlib import Path

import utils.my_globs as my_globs
from config import DataState
from src.pipelines.data_pipeline import DataPipeline
from utils.logging_config import setup_logging
from utils.setup_env import setup_project_env


def setup():
    """Set up project environment and logging."""
    project_dir, project_config = setup_project_env(log_file_name=__file__)
    setup_logging('MAIN', project_dir, f'{Path(__file__).stem}.log', project_config)
    logging.getLogger().setLevel(logging.DEBUG)

    my_globs.project_dir = project_dir
    my_globs.project_config = project_config


if __name__ == '__main__':
    setup()

    data_state = DataState()
    try:
        DataPipeline(data_state).main()

    except Exception as e:
        logging.error(f'Pipeline terminated due to unexpected error: {e}', exc_info=True)

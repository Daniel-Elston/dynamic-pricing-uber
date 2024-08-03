from __future__ import annotations

import logging

from config.data import DataConfig
from config.data import DataPaths
from config.data import DataState
from src.pipelines.data_pipeline import DataPipeline
from utils.project_setup import setup_project


if __name__ == '__main__':
    project_dir, project_config = setup_project()

    dc, dp = DataConfig(), DataPaths()
    ds = DataState(dp, dc)
    try:
        DataPipeline(ds, dc).main()
    except Exception as e:
        logging.error(f'Pipeline terminated due to unexpected error: {e}', exc_info=False)

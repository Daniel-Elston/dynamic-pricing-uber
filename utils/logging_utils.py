from __future__ import annotations

import logging
import time
from functools import wraps
from pprint import pformat
from typing import Callable


def log_step(load_path: str = None, save_paths: str = None, view: bool = False, input: bool = False, output: bool = False):
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            script_name = func.__module__.split('.')[-1]
            class_name = func.__qualname__.split('.')[0]
            func_name = func.__name__
            locate = f'`{script_name}.{class_name}.{func_name}`'
            logging.info(f"STARTING {locate} INITIATING")
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                if load_path:
                    logging.info(f"Load path of: {pformat(load_path)}")
                if save_paths:
                    logging.info(f"Save path of: {pformat(save_paths)}")
                if view:
                    logging.info(f"Load path of {locate}:\n{pformat(load_path)}")
                    logging.info(f"Save path of {locate}:\n{pformat(save_paths)}")
                    logging.info(f"Args of {locate}:\n{pformat(args)}")
                    logging.info(f"Kwargs of {locate}:\n{pformat(kwargs)}")
                    logging.info(f"Result of {locate}:\n{pformat(result)}")
                if input:
                    logging.info(f"Args of {locate}:\n{pformat(args)}")
                    logging.info(f"Kwargs of {locate}:\n{pformat(kwargs)}")
                if output:
                    logging.info(f"Result of {locate}:\n{pformat(result)}")
                logging.info(f"COMPLETED {locate} SUCCESSFULLY\n")
                return result
            except Exception as e:
                logging.exception(f"Error in {locate}: {str(e)}")
                raise
            finally:
                end_time = time.time()
                duration = end_time - start_time
                logging.debug(f"{locate} took {duration:.2f} seconds to execute")
        return wrapper
    return decorator

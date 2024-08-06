from __future__ import annotations

import logging
import time
from functools import wraps
from pathlib import Path
from pprint import pformat
from typing import Callable
from typing import List
from typing import Optional
from typing import Union


def log_step(
        load_path: Optional[Union[str, Path]] = None,
        save_paths: Optional[Union[str, Path, List[Union[str, Path]]]] = None,
        view: bool = False,
        input: bool = False,
        output: bool = False):
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

                log_details = {
                    "Load path": load_path if load_path or view or input else None,
                    "Save paths": save_paths if save_paths or view or output else None,
                    "Args": args if input or view else None,
                    "Kwargs": kwargs if input or view else None,
                    "Result": result if output or view else None
                }

                for key, value in log_details.items():
                    if value is not None:
                        if isinstance(value, (str, Path)):
                            logging.info(f"{key}: {value}")
                        elif isinstance(value, list):
                            logging.info(f"{key}:\n{pformat(value)}")
                        else:
                            logging.info(f"{key}:\n{pformat(value)}")

                logging.info(f"COMPLETED {locate} SUCCESSFULLY\n")
                return result
            except Exception as e:
                logging.exception(f"Error in {locate}: {str(e)}")
                raise
            finally:
                duration = time.time() - start_time
                logging.debug(f"{locate} took {duration:.2f} seconds to execute")

        return wrapper
    return decorator

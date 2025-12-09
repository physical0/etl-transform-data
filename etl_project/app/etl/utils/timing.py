import time
from functools import wraps
from app.etl.core.logging_config import logger

def time_execution(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        logger.info(f"Starting {func.__name__}...")
        result = func(*args, **kwargs)
        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"Finished {func.__name__} in {duration:.2f} seconds")
        return result
    return wrapper

import functools
import logging


def log(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logging.info(f"{func.__name__} 开始: {func.__doc__}")
        func(*args, **kwargs)
        logging.info(f"{func.__name__} 结束")

    return wrapper

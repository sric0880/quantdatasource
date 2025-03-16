import functools
import logging


def log(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logging.info(f"{func.__class__.__name__}.{func.__name__} 开始: {func.__doc__}")
        try:
            func(*args, **kwargs)
        except Exception as e:
            logging.error(exc_info=e, stack_info=True)
        logging.info(f"{func.__class__.__name__}.{func.__name__} 结束")

    return wrapper

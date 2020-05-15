from typing import Callable


def crontab(pattern: str):
    def wrapper(func: Callable):
        def inner_wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        return inner_wrapper
    return wrapper
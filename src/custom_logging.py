import functools
import logging
import pprint

log = logging.getLogger(__name__)


def log_call(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        log.debug(
            f"{func.__name__}: called with\nargs=\n{pprint.pformat(args)}\nkwargs=\n{pprint.pformat(kwargs)}"
        )
        result = func(*args, **kwargs)
        log.debug(f"{func.__name__}: returned\n{pprint.pformat(result)}")
        return result

    return wrapper

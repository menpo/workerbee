import datetime
import logging
import time
import sys


DEFAULT_LOGGER = logging.getLogger('workerbee')
# Clear default handlers (perhaps set my BasicConfig)
DEFAULT_LOGGER.handlers = []
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setLevel(logging.INFO)
stdout_handler.setFormatter(
    logging.Formatter('%(levelname)-8s (%(asctime)-5s): %(message)s',
                      '%Y-%m-%d %H:%M:%S'))
DEFAULT_LOGGER.addHandler(stdout_handler)
DEFAULT_LOGGER.setLevel(logging.INFO)
del stdout_handler
# Replace with name so that we use getLogger
DEFAULT_LOGGER = 'workerbee'


class timer:
    r"""
    Context manager for timing a Python function. Stores the interval as a
    datetime.timedelta.
    """
    def __enter__(self):
        # Record wall time
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.interval = datetime.timedelta(seconds=self.end - self.start)


def exponential_decay(base=2, max_value=None):
    """Generator for exponential decay.

    Parameters
    ----------
    base : `int`, optional
        The base of the exponentiation.
    max_value : `int`, optional
        The maximum value to yield. Once the value in the sequence exceeds
        this, the value of ``max_value`` will continuously yielded.
    """
    n = 0
    while True:
        a = base ** n
        if max_value is None or a < max_value:
            yield a
            n += 1
        else:
            yield max_value

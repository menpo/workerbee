import datetime
import logging
import random
import time
import sys
from functools import partial


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


class JobsExhaustedError(ValueError):
    pass


class JobFailed(ValueError):
    pass


def exhaust_all(next_job_f, todo_f, done_f, perform_job_f, verbose=False):
    if verbose:
        i = 0
        # find out how many jobs there are in total for nice formatting
        padding = len(str(len(list(todo_f()))))
    try:
        while True:
            if verbose:
                sys.stdout.write(str(i).zfill(padding) + ': ')
                sys.stdout.flush()
            a_job = next_job_f(todo_f(), done_f())
            if verbose:
                sys.stdout.write("acquired '{}'".format(a_job))
                sys.stdout.flush()
            perform_job_f(a_job)
            if verbose:
                sys.stdout.write('...done.\n')
                sys.stdout.flush()
                i += 1
    except JobsExhaustedError:
        sys.stdout.write('\rJobs exhausted')
        sys.stdout.flush()


def next_job_with_choice(choose_f, remaining_jobs_f, todo, done):
    return choose_f(remaining_jobs_f(todo, done))


def choose_randomly(jobs):
    if len(jobs) > 0:
        return random.sample(jobs, 1)[0]
    else:
        raise JobsExhaustedError("Out of jobs")


random_next_job = partial(next_job_with_choice, choose_randomly)

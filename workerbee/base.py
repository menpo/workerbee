import random
from functools import partial
from pathlib import Path
import sys


class JobsExhaustedError(ValueError):
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


def next_job_with_choice(choose_f, remaining_f, todo, done):
    return choose_f(remaining_f(todo, done))


def choose_randomly(jobs):
    if len(jobs) > 0:
        return random.sample(jobs, 1)[0]
    else:
        raise JobsExhaustedError("Out of jobs")


as_paths = lambda p: [Path(i) for i in p]


def filename_difference(a, b):
    paths_a, paths_b = as_paths(a), as_paths(b)
    src_name_to_path = {a.name: a for a in paths_a}
    if len(src_name_to_path) != len(paths_a):
        raise ValueError('Source paths are not uniquely identified by names')
    remaining_names = set(src_name_to_path) - set(i.name for i in paths_b)
    return [src_name_to_path[i] for i in remaining_names]


random_next_job = partial(next_job_with_choice, choose_randomly)
random_next_filename = partial(random_next_job, filename_difference)
exhaust_all_files_randomly = partial(exhaust_all, random_next_filename)

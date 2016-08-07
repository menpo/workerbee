from functools import partial
import random
import sys


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

import re

from functools import wraps


class JobsExhaustedError(ValueError):
    r"""
    This exception is thrown by a jobset worker when all jobs are completed. It
    allows the escape of the otherwise infinite loop of receiving and processing
    jobs.
    """
    pass


class JobFailedError(ValueError):
    r"""
    This exception should be thrown by a user's custom processing function if
    for any reason the function is unable to complete the job provided to it.
    In this case, the job's ``n_failed_attempts`` counter is increased and the
    job re-enters the pool of available uncompleted and unclaimed jobs.
    """
    pass


class UniqueInputDataConstraintError(ValueError):
    PGERROR_REGEX = re.compile("Key \(\(input_data ->> 'id'::text\)\)=\((.*)\) already exists.")

    def __init__(self, pg_error, msg=None):
        if msg is None:
            detail_str = pg_error.pgerror.split('DETAIL:')[1].strip()
            matches = self.PGERROR_REGEX.match(detail_str)
            if matches:
                msg = "input_data with id='{}' already exists".format(
                    matches.group(1))
        super(UniqueInputDataConstraintError, self).__init__(msg)
        self.pg_error = pg_error


def catch_all_exceptions(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except:  # Catch ALL exceptions
            raise JobFailedError()
    return wrapper

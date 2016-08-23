
class JobsExhaustedError(ValueError):
    r"""
    This exception is thrown by a jobset worker when all jobs are completed. It
    allows the escape of the otherwise infinite loop of receiving and processing
    jobs.
    """
    pass


class JobFailed(ValueError):
    r"""
    This exception should be thrown by a user's custom processing function if
    for any reason the function is unable to complete the job provided to it.
    In this case, the job's ``n_failed_attempts`` counter is increased and the
    job re-enters the pool of available uncompleted and unclaimed jobs.
    """
    pass

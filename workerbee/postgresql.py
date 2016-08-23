from __future__ import division

import logging
import os
import sys
import time
from distutils.version import StrictVersion
from string import ascii_letters, digits

import psycopg2
from postgres import Postgres
from psycopg2.extras import Json as postgres_jsonify

from .base import DEFAULT_LOGGER, exponential_decay, timer
from .exceptions import JobFailed, JobsExhaustedError
from .stats import get_stats_report

if sys.version_info.major == 3:
    string_types = (str,)
else:
    string_types = (basestring,)

ALLOWED_CHARACTERS_IN_TABLE_NAME = set(ascii_letters.lower()) | set(digits) | set('_')
JSON_POSTGRES_MIN_VER = StrictVersion('9.2')
JSON_POSTGRES_OP_VER = StrictVersion('9.3')
JSONB_POSTGRES_VER = StrictVersion('9.4')

# According to the postgres.py documentation, we should only have a single
# instantiation of the 'Postgres' class per database connection, per-process.
# So we need an ugly global to store the handles - which will be instantiated
# whenever the first db accessing method is called and is indexed by the
# connection info.
DB_HANDLES = {}


###############################################################################


POSTGRES_CHECK_VERSION = r"""
SELECT version()
"""

TABLE_EXISTS_QUERY = r"""
SELECT EXISTS(
    SELECT *
    FROM information_schema.tables
    WHERE table_name = '{tbl_name}'
)
""".strip()

CREATE_TABLE_QUERY = r"""
CREATE TABLE {tbl_name}(
  id SERIAL PRIMARY KEY,
  input_data JSON{jsonb} NOT NULL,
  output_data JSON,
  n_claims INTEGER NOT NULL DEFAULT 0,
  time_last_completed TIMESTAMP WITH TIME ZONE,
  time_last_claimed TIMESTAMP WITH TIME ZONE,
  job_duration INTERVAL,
  n_failed_attempts INTEGER NOT NULL DEFAULT 0
)
""".strip()

UNIQUE_INPUT_DATA_CONSTRAINT = r"""
ALTER TABLE {tbl_name} ADD UNIQUE (input_data);
""".strip()

UNIQUE_INPUT_DATA_ID_CONSTRAINT = r"""
CREATE UNIQUE INDEX {tbl_name}_uq_input_data_id
ON {tbl_name} ((input_data ->> 'id'))
""".strip()

INSERT_JOB_QUERY = r"""
INSERT INTO {tbl_name} (input_data) VALUES (%(input_data)s)
""".strip()

UNCOMPLETED_UNCLAIMED_ROW_QUERY = r"""
SELECT *
FROM {tbl_name} WHERE time_last_completed ISNULL AND time_last_claimed ISNULL
LIMIT 1;
""".strip()

OLDEST_UNCOMPLETED_ROW_QUERY = r"""
SELECT *
FROM {tbl_name}
WHERE time_last_completed ISNULL AND n_failed_attempts < %(max_n_retry_attempts)s
ORDER BY time_last_claimed
LIMIT 1;
""".strip()

SET_ROW_COMPLETED_BY_ID_QUERY = r"""
UPDATE {tbl_name}
SET time_last_completed=CURRENT_TIMESTAMP, output_data=%(output_data)s, job_duration=%(job_duration)s
WHERE id=%(id)s
""".strip()

SET_ROW_CLAIMED_BY_ID_QUERY = r"""
UPDATE {tbl_name} SET time_last_claimed=CURRENT_TIMESTAMP, n_claims = n_claims + 1
WHERE id=%(id)s
""".strip()

UPDATE_ROW_N_FAILED_ATTEMPTS_BY_ID_QUERY = r"""
UPDATE {tbl_name} SET n_failed_attempts = n_failed_attempts + 1
WHERE id=%(id)s
""".strip()

TOTAL_ROWS_QUERY = r"""
SELECT COUNT(*) FROM {tbl_name}
""".strip()

COMPLETED_ROWS_QUERY = r"""
SELECT COUNT(*) FROM {tbl_name} WHERE time_last_completed NOTNULL
""".strip()


###############################################################################


class DBConnectionInfo(object):

    def __init__(self, host=None, port=None, user=None, password=None,
                 dbname=None):
        self.host = host or os.environ.get('PGHOST', None)
        self.port = port or os.environ.get('PGPORT', None)
        self.user = user or os.environ.get('PGUSER', None)
        self.dbname = dbname or os.environ.get('PGDATABASE', None)
        self.password = password

    def missing_info(self):
        return None in {self.host, self.port, self.dbname, self.user}

    def postgres_connection_string(self):
        conn_str = 'host={host} port={port} user={user} dbname={db}'.format(
            db=self.dbname, user=self.user, host=self.host, port=self.port)
        if self.password:
            conn_str += ' password={}'.format(self.password)
        return conn_str

    def __eq__(self, other):
        return (isinstance(other, self.__class__) and
                self.__dict__ == other.__dict__)

    def __hash__(self):
        # Don't hash on the password.
        # Ensure that objects with the same connection info will hash the same
        return hash((self.host, self.port, self.user, self.dbname))

    def __str__(self):
        if self.password:
            pass_len = len(self.password)
            starred_pass = ('*' * (pass_len))
            if pass_len > 3:
                starred_pass = starred_pass[:-2] + self.password[-2:]
            passw = ':{}'.format(starred_pass)
        else:
            passw = ''
        return '{user}{passw}@{host}:{port}/{db}'.format(
            db=self.dbname, user=self.user, host=self.host, port=self.port,
            passw=passw)


def get_postgres_version(db_handle):
    pg_ver = db_handle.one(POSTGRES_CHECK_VERSION)
    # Slice off version. Example expected output:
    #   PostgreSQL 9.2.10 on x86_64-unknown-linux-gnu, compiled by gcc (Ubuntu/Linaro 4.6.3-1ubuntu5) 4.6.3, 64-bit
    return StrictVersion(pg_ver.split(' ')[1])


def get_db_handle(db_info=None, logger_name=DEFAULT_LOGGER):
    if db_info is None:
        db_info = DBConnectionInfo()

    if db_info in DB_HANDLES:
        handle = DB_HANDLES[db_info]
    else:
        if db_info.missing_info():
            raise ValueError('Unable to find the database configuration in the '
                             'local environment.')

        logger = logging.getLogger(logger_name)
        logger.info('Creating connection pool for {}'.format(db_info))
        if db_info.password is not None:
            logger.warn('Password is set via keyword argument. Note that this '
                        'is insecure and a ~/.pgpass file should be preferred.')
        else:
            logger.info('No password is set - the default behaviour of probing '
                        'the ~/.pgpass or PGPASSWORD environment variable '
                        'will be used.')

        DB_HANDLES[db_info] = Postgres(db_info.postgres_connection_string())
        handle = DB_HANDLES[db_info]
    return handle


def check_valid_table_name(table_name):
    if not isinstance(table_name, string_types):
        raise TypeError("Experiment ID '{}' is of type {}, not string".format(
            table_name, type(table_name)))
    invalid = set(table_name) - ALLOWED_CHARACTERS_IN_TABLE_NAME
    if len(invalid) > 0:
        invalid_c = ', '.join(["'{}'".format(l) for l in sorted(list(invalid))])
        raise ValueError("Invalid characters in experiment ID: {} "
                         "(allowed [a-z0-9_]+)".format(invalid_c))


def table_exists(db_handle, tbl_name):
    return db_handle.one(TABLE_EXISTS_QUERY.format(tbl_name=tbl_name),
                         back_as=dict)


def create_table(db_handle, tbl_name, logger_name=DEFAULT_LOGGER):
    logger = logging.getLogger(logger_name)
    pg_ver = get_postgres_version(db_handle)
    if pg_ver < JSON_POSTGRES_MIN_VER:
        raise ValueError('Minimum postgresql version requirement was not met '
                         '({} < {})'.format(pg_ver, JSON_POSTGRES_MIN_VER))
    # If Postgres >= 9.4 then create use jsonb as the 'input_data' data type
    if pg_ver >= JSONB_POSTGRES_VER:
        jsonb_input = 'b'
        logger.info('Found Postgresql version {} - Using JSONB as the data '
                    'type for the input_data field.'.format(pg_ver))
    else:
        jsonb_input = ''
        logger.info('Found Postgresql version {} - Using JSON as the data '
                    'type for the input_data field.'.format(pg_ver))
    with db_handle.get_cursor() as cursor:
        cursor.run(CREATE_TABLE_QUERY.format(tbl_name=tbl_name,
                                             jsonb=jsonb_input))
        # If Postgres >= 9.3 then create a unique constraint on the reserved
        # id key in the 'input_data'
        if JSON_POSTGRES_OP_VER <= pg_ver < JSONB_POSTGRES_VER:
            logger.info("Adding UNIQUE constraint to JSON field input_"
                        "data->>'id'")
            cursor.run(UNIQUE_INPUT_DATA_ID_CONSTRAINT.format(tbl_name=tbl_name))
        elif pg_ver >= JSONB_POSTGRES_VER:
            logger.info("Adding UNIQUE constraint to JSONB field input_data")
            cursor.run(UNIQUE_INPUT_DATA_CONSTRAINT.format(tbl_name=tbl_name))
        else:
            logger.warn("NO UNIQUE constraint enforced")


def get_total_job_count(db_handle, tbl_name):
    return db_handle.one(TOTAL_ROWS_QUERY.format(tbl_name=tbl_name))


def get_completed_job_count(db_handle, tbl_name):
    return db_handle.one(COMPLETED_ROWS_QUERY.format(tbl_name=tbl_name))


################################################################################


class PostgresqlJobSet(object):
    r"""A set of jobs to complete managed by a PostgreSQL database.

    Provides access to managing a set of jobs with the given ``jobset_id``.
    The PostgreSQL table/infrastructure can be managed by this class and jobs
    can be added at any time. This class should also be used in order to process
    the jobs for the given jobset, using the :meth:`run` method.

    Note that the input and output data are stored in PostgreSQL using the
    JSON type and thus the Python `dict` objects provided/returned must be
    JSON serializable.

    PostgreSQL >= 9.2 is required as the JSON type is utilized for data
    storage. PostgreSQL >= 9.4 is preferred as job data should be unique
    for a given jobset. If  PostgreSQL == 9.2 then no UNIQUE constraints
    are applied and this may not be ideal for complex cluster based job
    systems. If PostgreSQL == 9.3 the UNIQUE constraint is enforced via a
    reserved `id` key in the ``input_data`` field - which should be unique to
    each job in a jobset.

    Parameters
    ----------
    jobset_id : `str`
        The ID of the jobset, which will be used as the table name in the
        PostgreSQL database. Therefore, the ID may only contain lowercase
        ASCII characters.
    host : `str`, optional
        The PostgreSQL host address. If not provided, the PostgreSQL driver will
        attempt to infer this value from the ``PGHOST`` environment variable.
    port : `str`, optional
        The PostgreSQL host port. If not provided, the PostgreSQL driver will
        attempt to infer this value from the ``PGPORT`` environment variable.
    user : `str`, optional
        The PostgreSQL user. If not provided, the PostgreSQL driver will
        attempt to infer this value from the ``PGUSER`` environment variable.
    password : `str`, optional
        The PostgreSQL user password. This is provided for convenience but
        should be avoided where possible for security reasons. If not provided,
        the PostgreSQL driver will attempt to infer this value from the
        ``~/.pgpass`` file. If this file does not exist, the (not recommended)
        ``PGPASSWORD`` environment variable will be used.
    dbname : `str`, optional
        The PostgreSQL database name. If not provided, the PostgreSQL driver
        will attempt to infer this value from the ``PGDATABASE`` environment
        variable.
    logger_name : `str`, optional
        The name of a Python logging module logger. A default logger
        ('workerbee') is provided which only prints to stdout.

    Examples
    --------
    A short very basic example of a dummy jobset.

    >>> def process(input_data):
    >>>     if input_data['id'] % 2 == 0:
    >>>       return input_data['id'] * 2
    >>>     else:
    >>>       return input_data['id']

    >>> experiment = PostgresqlJobSet('example_id', host='127.0.0.1',
    >>>                               user='postgres', password='password',
    >>>                               dbname='postgres')
    >>> experiment.setup_jobset()
    >>> experiment.add_jobs({'id': i + 1 for i in range(10)})  # Add 10 'jobs'
    >>> # Do not consider failed experiments (max_n_retry_attempts=-1)
    >>> experiment.run(process, max_n_retry_attempts=-1)
    """
    def __init__(self, jobset_id, host=None, port=None, user=None,
                 password=None, dbname=None, logger_name=DEFAULT_LOGGER):
        self.jobset_id = jobset_id
        self.logger_name = logger_name
        self.logger = logging.getLogger(logger_name)

        # Validate jobset id is valid postgresql table name
        check_valid_table_name(self.jobset_id)

        self.db_connection_info = DBConnectionInfo(
            host=host, port=port, user=user, password=password,
            dbname=dbname)
        self.db_handle = get_db_handle(self.db_connection_info,
                                       logger_name=logger_name)

    def setup_jobset(self, ignore_existing_jobset=False):
        r"""Create the PostgreSQL table for the jobset ID.

        This sets up the table structure for the jobset ID defined at
        instantiation of this object. No jobs are added at construction, please
        use :meth:`add_job`/:meth:`add_jobs`.

        Parameters
        ----------
        ignore_existing_jobset : `bool`, optional
            If ``True``, ignore if the table for this jobset ID already exists.
            A warning will be printed to the logger.

        Raises
        ------
        ValueError
            If ``ignore_existing_jobset==False`` and the jobset already exists.
        """
        if self.does_jobset_exist() and not ignore_existing_jobset:
            msg = "Table already exists for jobset '{}'".format(self.jobset_id)
            self.logger.error(msg)
            raise ValueError(msg)
        else:
            try:
                create_table(self.db_handle, self.jobset_id)
                self.logger.info("Created table for jobset '{}'".format(
                    self.jobset_id))
            except psycopg2.ProgrammingError as e:
                if not ignore_existing_jobset:
                    # Re-raise - otherwise swallow
                    raise e
                else:
                    self.logger.warn("Table already exists for jobset '{}' - "
                                     "but ignoring".format(self.jobset_id))

    def does_jobset_exist(self):
        r"""Returns ``True`` if the jobset exists.

        Returns
        -------
        exists : `bool`
            ``True`` if a table exists for the ``jobset_id``.
        """
        return table_exists(self.db_handle, self.jobset_id)

    def add_job(self, input_data, cursor=None):
        r"""Add a single job to the jobset.

        Adds a single job to the jobset. The optional ``cursor`` can be used
        in order to submit multiple jobs as part of a transaction.

        Note that the input data are stored in PostgreSQL using the
        JSON type and thus the Python `dict` objects provided must be
        JSON serializable.

        The ``input_data`` MUST BE UNIQUE in the jobset. For PostgreSQL < 9.4
        this is enforced by a reserved key 'id' in the jobset. Set this
        value to a unique value per job in order to enforce the uniqueness
        constraint.

        Parameters
        ----------
        input_data : `dict`
            The input data to be passed to the user's processing function. The
            ``dict`` should only contain Python objects that are serializable
            to a JSON representation.
        cursor : `postgres.CursorContextManager`, optional
            If provided, the cursor will be used to submit the job, otherwise
            a new transaction will be created.
        """
        query_str = INSERT_JOB_QUERY.format(tbl_name=self.jobset_id)
        params = {'parameters': {'input_data': postgres_jsonify(input_data)}}
        if cursor is None:
            self.db_handle.run(query_str, **params)
        else:
            cursor.run(query_str, **params)

    def add_jobs(self, input_datas, cursor=None):
        r"""Add multiple jobs to the jobset as a single transaction.

        Adds multiple jobs to the jobset. The optional ``cursor`` can be used
        in order to submit multiple jobs as part of a transaction. If any job
        fails to be added for any reason, then no jobs are added.

        Note that the input data are stored in PostgreSQL using the
        JSON type and thus the Python `dict` objects provided must be
        JSON serializable.

        The ``input_data`` MUST BE UNIQUE in the jobset. For PostgreSQL < 9.4
        this is enforced by a reserved key 'id' in the jobset. Set this
        value to a unique value per job in order to enforce the uniqueness
        constraint.

        Parameters
        ----------
        input_datas : `list` of `dict`
            The input datas to be passed to the user's processing function. The
            ``dict`` should only contain Python objects that are serializable
            to a JSON representation.
        cursor : `postgres.CursorContextManager`, optional
            If provided, the cursor will be used to submit the jobs, otherwise
            a new transaction will be created.
        """
        if cursor is None:
            with self.db_handle.get_cursor() as cursor:
                for input_data in input_datas:
                    self.add_job(input_data, cursor=cursor)
        else:
            for input_data in input_datas:
                self.add_job(input_data, cursor=cursor)
        self.logger.info('Submitted {} jobs'.format(len(input_datas)))

    def _set_job_as_complete(self, job_id, duration, output_data=None):
        r"""Set a job as completed.

        Once completed, the job will not be re-computed by any other workers.

        Parameters
        ----------
        job_id : `str`
            The PostgreSQL id of the job.
        duration : `datetime.timedelta`
            The duration, in floating point seconds, of the job.
        output_data : `dict`, optional
            The output data returned from the user's processing function. The
            ``dict`` should only contain Python objects that are serializable
            to a JSON representation.
        """
        if output_data is not None:
            output_data = postgres_jsonify(output_data)
        params = {
            'id': job_id,
            'job_duration': duration,
            'output_data': output_data
        }
        self.db_handle.run(SET_ROW_COMPLETED_BY_ID_QUERY.format(
            tbl_name=self.jobset_id), parameters=params)

    def _get_uncompleted_unclaimed_job(self):
        r"""Get a totally unprocessed job.

        An uncompleted, unclaimed job has not been considered by any worker so
        far.

        Returns
        -------
        job : `nametuple`
            A `namedtuple` containing all of the columns for an uncompleted
            unclaimed row.
        """
        return self.db_handle.one(UNCOMPLETED_UNCLAIMED_ROW_QUERY.format(
            tbl_name=self.jobset_id))

    def _get_oldest_uncompleted_job(self, max_n_retry_attempts):
        r"""Get an uncompleted job.

        An uncompleted job may have been previously claimed. However, since
        Workerbee workers are not fault tolerant, the worker may have failed
        or may be unusually slow to process. Therefore, jobs may be repeated
        by workers until such time as a worker marks the job as completed.
        Therefore, this method returns the oldest uncompleted job which is
        reasonably assumed to be the most likely candidate for re-processing.
        Note that a given job may have been explicitly marked by a previous
        worker as having failed - but this failure may be due to infrastructure
        issues and therefore we allow re-processing of previously failed jobs,
        up to a maximum number of attempts.

        Parameters
        ----------
        max_n_retry_attempts : `int`
            The maximum number of retry attempts. Jobs may fail for a variety
            of reasons and therefore we permit jobs to be retried up to a
            maximum number of times.

        Returns
        -------
        job : `nametuple`
            A `namedtuple` containing all of the columns for an uncompleted
            row.
        """
        params = {'max_n_retry_attempts': max_n_retry_attempts}
        return self.db_handle.one(OLDEST_UNCOMPLETED_ROW_QUERY.format(
            tbl_name=self.jobset_id), parameters=params)

    def _update_job_n_failed_attempts(self, job_id):
        r"""Update the failure count for the job.

        Jobs may fail for a variety of reasons and therefore we permit jobs to
        be retried up to a maximum number of times. This increments the failed
        counter for a given job.

        Parameters
        ----------
        job_id : `str`
            The PostgreSQL id of the job
        """
        self.db_handle.run(UPDATE_ROW_N_FAILED_ATTEMPTS_BY_ID_QUERY.format(
            tbl_name=self.jobset_id), parameters={'id': job_id})

    def _set_job_as_claimed(self, job_id):
        r"""Mark the job as claimed.

        Workerbee jobs are assumed to be idempotent and therefore repeating a
        job is permitted. Marking a job as claimed ensures no other worker
        will claim this job unless all other uncompleted jobs are claimed.

        Parameters
        ----------
        job_id : `str`
            The PostgreSQL id of the job
        """
        self.db_handle.run(SET_ROW_CLAIMED_BY_ID_QUERY.format(
            tbl_name=self.jobset_id), parameters={'id': job_id})

    def run(self, job_callable, busywait=False, max_busywait_sleep=None,
            max_failure_sleep=None, max_n_retry_attempts=10):
        r"""Process the jobset with the given callable.

        Use the given callable/function to process the uncompleted jobs
        in the jobset. The user function should have the following signature:

            job_callable(input_data: dict) -> dict

        And may optionally return a JSON serializable `dict` that stores
        the result of the job. In the event of a failure, the job should
        raise a ``JobFailed`` exception. Workerbee jobs are expected to be
        deterministic and idempotent as Workerbee jobs may be processed
        multiple times. By default, the worker will terminate if all jobs
        are either marked as completed or violate the provided maximum
        number of failures.

        Parameters
        ----------
        job_callable : `callable` : job_callable(input_data: dict) -> dict
            A `callable`/`function` that expects a single argument, a Python
            dictionary, containing the data required to process the job. The
            callable should process the job as required and may optionally
            return a JSON serializable Python `dict` as output data that
            will be stored as the result of the job. In result of failure,
            the callable should raised a `JobFailed` exception. Workerbee
            expects the result of the callable to be deterministic and
            idempotent as a given job may be run multiple times.
        busywait : `bool`, optional
            If ``True``, busy wait when all jobs appear to be completed. The
            busy waiting is implemented by sleeping with a binary exponential
            backoff to reduce database load.
        max_busywait_sleep : `int`, optional
            The maximum amount of time to sleep if ``busywait==True``. This
            prevents long running workers from sleeping for excessive periods.
        max_failure_sleep : `int`, optional
            When a job fails, the worker will sleep for a binary exponential
            backoff in order to reduce database load. This sets the maximum
            amount of time to sleep, to prevent workers from sleeping for
            excessive periods.
        max_n_retry_attempts : `int`, optional
            The maximum number of times a job may have failed (exclusive) for
            the job to still be considered unclaimed. This enables different
            workers to be more 'fault tolerant' than others. To not consider
            any failed jobs, set this to ``-1``.

        Examples
        --------
        A short very basic example of a custom user function - assuming
        the input data consists of a dictionary containing a single key
        `id` mapping to an integer.

        >>> def process(input_data):
        >>>     if input_data['id'] % 2 == 0:
        >>>       return input_data['id'] * 2
        >>>     else:
        >>>       return input_data['id']

        >>> experiment = PostgresqlJobSet('example_id', host='127.0.0.1',
        >>>                               user='postgres', password='password',
        >>>                               dbname='postgres')
        >>> # Assume setup_jobset and add_jobs have been called previously.
        >>> # Do not consider failed jobs (max_n_retry_attempts=-1)
        >>> experiment.run(process, max_n_retry_attempts=-1)
        """
        busywait_decay = exponential_decay(max_value=max_busywait_sleep)
        fail_decay = exponential_decay(max_value=max_failure_sleep)
        if max_n_retry_attempts is None:
            max_n_retry_attempts = -1

        try:
            while True:
                a_row = self._get_uncompleted_unclaimed_job()
                if a_row is None:
                    # there is nothing left that is unclaimed, so we may as well
                    # 'repeat' already claimed work - maybe we can beat another
                    # worker to complete it.
                    self.logger.info('No unclaimed work remains - re-claiming '
                                     'oldest job')
                    a_row = self._get_oldest_uncompleted_job(max_n_retry_attempts)

                if a_row is None:
                    if busywait:
                        d = next(busywait_decay)
                        self.logger.info('No uncompleted work - busywait with '
                                         'binary exponential decay '
                                         '({} seconds)'.format(d))
                        time.sleep(d)
                    else:
                        # Break out of the infinite loop (self caught).
                        raise JobsExhaustedError()
                else:
                    # Reset the busywait decay
                    busywait_decay = exponential_decay(
                        max_value=max_busywait_sleep)

                    # let's claim the job
                    self._set_job_as_claimed(a_row.id)

                    self.logger.info('Claimed job (id: {})'.format(a_row.id))
                    try:
                        with timer() as t:
                            output_data = job_callable(a_row.input_data)
                    except JobFailed:
                        d = next(fail_decay)
                        self._update_job_n_failed_attempts(a_row.id)
                        self.logger.warn('Failed to complete job (id: {}) - '
                                         'sleeping with binary exponential '
                                         'decay ({}s)'.format(a_row.id, d))
                        time.sleep(d)
                    else:
                        # Reset the failure decay
                        fail_decay = exponential_decay(
                            max_value=max_failure_sleep)

                        # Update the job information
                        self._set_job_as_complete(a_row.id, t.interval,
                                                  output_data=output_data)
                        total_secs = t.interval.total_seconds()
                        self.logger.info('Completed job (id: {}) in {:.2f} '
                                         'seconds'.format(a_row.id, total_secs))
        except JobsExhaustedError:
            self.logger.info('All jobs are exhausted, terminating.')

    def _stats_report(self):
        r"""Return stats report string.

        Statistics about the status of the job. Query the database for the
        following statistics ::

            n_jobs
            n_completed
            avg_job_duration
            jobs_per_second
            seconds remaining
            approx_finish_time

        Returns
        -------
        report : `str`
            Statistics about the status of the job.
        """
        return get_stats_report(self.db_handle, self.jobset_id)

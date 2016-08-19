from __future__ import division

import logging
import os
import sys

import time
from postgres import Postgres
from psycopg2.extras import Json as postgres_jsonify

from .base import JobsExhaustedError, JobFailed, DEFAULT_LOGGER, timer, \
    exponential_decay

from string import ascii_letters, digits
if sys.version_info.major == 3:
    from itertools import zip_longest
    string_types = (str,)
else:
    from itertools import izip_longest as zip_longest
    string_types = (basestring,)
ALLOWED_CHARACTERS_IN_TABLE_NAME = set(ascii_letters.lower()) | set(digits) | set('_')

# According to the postgres.py documentation, we should only have a single
# instantiation of the 'Postgres' class per database connection, per-process.
# So we need an ugly global to store the handles - which will be instantiated
# whenever the first db accessing method is called and is indexed by the
# connection info.
DB_HANDLES = {}


###############################################################################

TABLE_EXISTS_QUERY = r"""
SELECT EXISTS(
    SELECT *
    FROM information_schema.tables
    WHERE table_name = '{tbl_name}'
);
""".strip()

CREATE_TABLE_QUERY = r"""
CREATE TABLE {tbl_name}(
  id SERIAL PRIMARY KEY,
  unique_id TEXT UNIQUE,
  input_data json NOT NULL,
  output_data json,
  n_claims INTEGER NOT NULL DEFAULT 0,
  time_last_completed TIMESTAMP WITH TIME ZONE,
  time_last_claimed TIMESTAMP WITH TIME ZONE,
  job_duration INTERVAL,
  n_failed_attempts INTEGER NOT NULL DEFAULT 0
)
""".strip()

INSERT_JOB_QUERY = r"""
INSERT INTO {tbl_name} (unique_id, input_data) VALUES (%(unique_id)s, %(input_data)s)
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

    def __init__(self, host=None, port=None, user=None, database=None):
        self.host = host or os.environ.get('PGHOST', None)
        self.port = port or os.environ.get('PGPORT', None)
        self.user = user or os.environ.get('PGUSER', None)
        self.database = database or os.environ.get('PGDATABASE', None)

    def missing_info(self):
        return None in {self.host, self.port, self.database, self.user}

    def postgres_connection_string(self):
        return 'host={} port={} user={} dbname={}'.format(
            self.host, self.port, self.user, self.database)

    def __eq__(self, other):
        return (isinstance(other, self.__class__) and
                self.__dict__ == other.__dict__)

    def __hash__(self):
        # Ensure that objects with the same connection info will hash the same
        return hash((self.host, self.port, self.user, self.database))

    def __str__(self):
        return '{} on {}@{}:{}'.format(self.database, self.user, self.host,
                                       self.port)


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
        if 'PGPASS' in os.environ:
            logger.info('Password is set via environment variable PGPASS.')
        else:
            logger.warn('No password is set. (If needed, set the '
                        'environment variable PGPASS.)')

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
    return db_handle.one(TABLE_EXISTS_QUERY.format(tbl_name=tbl_name))


def get_uncompleted_unclaimed_job(db_handle, tbl_name):
    return db_handle.one(UNCOMPLETED_UNCLAIMED_ROW_QUERY.format(tbl_name=tbl_name))


def get_oldest_uncompleted_job(db_handle, tbl_name, max_n_retry_attempts):
    return db_handle.one(OLDEST_UNCOMPLETED_ROW_QUERY.format(tbl_name=tbl_name),
                         parameters={'max_n_retry_attempts': max_n_retry_attempts})


def set_job_as_complete(db_handle, tbl_name, job_id, duration,
                        output_data=None):
    if output_data is not None:
        output_data = postgres_jsonify(output_data)
    db_handle.run(SET_ROW_COMPLETED_BY_ID_QUERY.format(tbl_name=tbl_name),
                  parameters={'id': job_id, 'job_duration': duration,
                              'output_data': output_data})


def set_job_as_claimed(db_handle, tbl_name, job_id):
    db_handle.run(SET_ROW_CLAIMED_BY_ID_QUERY.format(tbl_name=tbl_name),
                  parameters={'id': job_id})


def update_job_n_failed_attempts(db_handle, tbl_name, job_id):
    db_handle.run(UPDATE_ROW_N_FAILED_ATTEMPTS_BY_ID_QUERY.format(tbl_name=tbl_name),
                  parameters={'id': job_id})


def get_total_job_count(db_handle, tbl_name):
    return db_handle.one(TOTAL_ROWS_QUERY.format(tbl_name=tbl_name))


def get_completed_job_count(db_handle, tbl_name):
    return db_handle.one(COMPLETED_ROWS_QUERY.format(tbl_name=tbl_name))

################################################################################


def add_job(experiment_id, input_data, unique_id=None, db_connection_info=None,
            logger_name=DEFAULT_LOGGER, cursor=None):
    query_str = INSERT_JOB_QUERY.format(tbl_name=experiment_id)
    params = {'parameters': {'unique_id': unique_id,
                             'input_data': postgres_jsonify(input_data)}}
    if cursor is None:
        db_handle = get_db_handle(db_info=db_connection_info,
                                  logger_name=logger_name)

        if not table_exists(db_handle, experiment_id):
            raise ValueError("Table does not exist for experiment '{}'".format(
                experiment_id))

        db_handle.run(query_str, **params)
    else:
        cursor.run(query_str, **params)


def add_jobs(experiment_id, input_datas, unique_ids=None,
             db_connection_info=None, logger_name=DEFAULT_LOGGER, cursor=None):
    logger = logging.getLogger(logger_name)
    if cursor is None:
        db_handle = get_db_handle(db_info=db_connection_info,
                                  logger_name=logger_name)

        if not table_exists(db_handle, experiment_id):
            raise ValueError("Table does not exist for experiment '{}'".format(
                experiment_id))

        with db_handle.get_cursor() as cursor:
            for input_data, unique_id in zip_longest(input_datas, unique_ids):
                add_job(experiment_id, input_data, unique_id=unique_id,
                        db_connection_info=db_connection_info,
                        logger_name=logger_name, cursor=cursor)
    else:
        for input_data, unique_id in zip_longest(input_datas, unique_ids):
            add_job(experiment_id, input_data, unique_id=unique_id,
                    db_connection_info=db_connection_info,
                    logger_name=logger_name, cursor=cursor)
    logger.info('Submitted {} jobs'.format(len(input_datas)))


def setup_experiment(experiment_id, input_datas, unique_ids=None,
                     db_connection_info=None, logger_name=DEFAULT_LOGGER):
    if unique_ids is None:
        unique_ids = []

    db_handle = get_db_handle(db_info=db_connection_info,
                              logger_name=logger_name)
    logger = logging.getLogger(logger_name)

    check_valid_table_name(experiment_id)
    does_table_exist = table_exists(db_handle, experiment_id)

    if unique_ids:
        if len(unique_ids) != len(input_datas):
            msg = ('Must provide a unique ID per job if unique_ids is '
                   'provided. ({} unique ids, {} jobs)'.format(len(unique_ids),
                                                               len(input_datas)))
            logger.critical(msg)
            raise ValueError(msg)

    if does_table_exist:
        logger.warn("Table already exists for experiment '{}'".format(experiment_id))
    else:
        logger.info("Creating table for experiment '{}'".format(experiment_id))
        with db_handle.get_cursor() as cursor:  # Single Transaction
            # Create table
            cursor.run(CREATE_TABLE_QUERY.format(tbl_name=experiment_id))
            logger.info("Adding {} jobs to experiment '{}'".format(
                len(input_datas), experiment_id))

            # Fill in list of jobs
            add_jobs(experiment_id, input_datas, unique_ids=unique_ids,
                     db_connection_info=db_connection_info,
                     logger_name=logger_name, cursor=cursor)
        logger.info("Experiment '{}' set up with {} jobs.".format(
            experiment_id, len(input_datas)))


def postgres_worker(experiment_id, job_function, db_connection_info=None,
                    logger_name=DEFAULT_LOGGER, busywait=False,
                    max_busywait_sleep=None, max_failure_sleep=None,
                    max_n_retry_attempts=10):
    """

    Parameters
    ----------
    experiment_id : `str`
        A unique identifier for an experiment. Must be a valid Postgres Table
        name (no whitespace etc).
    job_function : `callable` taking (job_data : {})
        A callable that performs a unit of work in this experiment. The
        function will be provided with two arguments - the ``id`` for this
        job (a string) and the data payload for this job (a dictionary). This
        function then uses these inputs to perform the relevant work for the
        experiment. If the function completes without error, the job will be
        automatically marked complete in the database.
    """
    db_handle = get_db_handle(db_info=db_connection_info,
                              logger_name=logger_name)
    logger = logging.getLogger(logger_name)
    busywait_decay = exponential_decay(max_value=max_busywait_sleep)
    fail_decay = exponential_decay(max_value=max_failure_sleep)

    try:
        while True:
            a_row = get_uncompleted_unclaimed_job(db_handle,
                                                  experiment_id)
            if a_row is None:
                # there is nothing left that is unclaimed, so we may as well
                # 'repeat' already claimed work - maybe we can beat another
                # worker to complete it.
                logger.info('No unclaimed work remains - re-claiming oldest job')
                a_row = get_oldest_uncompleted_job(db_handle, experiment_id,
                                                   max_n_retry_attempts)

            if a_row is None:
                if busywait:
                    d = next(busywait_decay)
                    logger.info('No uncompleted work - busywait with binary '
                                'exponential decay ({} seconds)'.format(d))
                    time.sleep(d)
                else:
                    raise JobsExhaustedError()
            else:
                # Reset the busywait
                busywait_decay = exponential_decay(max_value=max_busywait_sleep)

                # let's claim the job
                set_job_as_claimed(db_handle, experiment_id, a_row.id)

                logger.info('Claimed job (id: {})'.format(a_row.id))
                try:
                    with timer() as t:
                        output_data = job_function(a_row.input_data,
                                                   unique_id=a_row.unique_id)
                except JobFailed as e:
                    d = next(fail_decay)
                    update_job_n_failed_attempts(db_handle, experiment_id, a_row.id)
                    logger.warn('Failed to complete job (id: {}) - sleeping '
                                'with binary exponential decay ({} seconds)'.format(
                                    a_row.id, d))
                    time.sleep(d)
                else:
                    # Reset the failure decay
                    fail_decay = exponential_decay(max_value=max_failure_sleep)

                    # Update the job information
                    set_job_as_complete(db_handle, experiment_id,
                                        a_row.id, t.interval,
                                        output_data=output_data)
                    logger.info('Completed job (id: {}) in {:.2f} seconds'.format(
                        a_row.id, t.interval.total_seconds()))
    except JobsExhaustedError:
        logger.info('All jobs are exhausted, terminating.')

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
        return table_exists(self.db_handle, self.jobset_id)

    def add_job(self, input_data, cursor=None):
        query_str = INSERT_JOB_QUERY.format(tbl_name=self.jobset_id)
        params = {'parameters': {'input_data': postgres_jsonify(input_data)}}
        if cursor is None:
            self.db_handle.run(query_str, **params)
        else:
            cursor.run(query_str, **params)

    def add_jobs(self, input_datas, cursor=None):
        if cursor is None:
            with self.db_handle.get_cursor() as cursor:
                for input_data in input_datas:
                    self.add_job(input_data, cursor=cursor)
        else:
            for input_data in input_datas:
                self.add_job(input_data, cursor=cursor)
        self.logger.info('Submitted {} jobs'.format(len(input_datas)))

    def set_job_as_complete(self, job_id, duration, output_data=None):
        if output_data is not None:
            output_data = postgres_jsonify(output_data)
        params = {
            'id': job_id,
            'job_duration': duration,
            'output_data': output_data
        }
        self.db_handle.run(SET_ROW_COMPLETED_BY_ID_QUERY.format(
            tbl_name=self.jobset_id), parameters=params)

    def get_uncompleted_unclaimed_job(self):
        return self.db_handle.one(UNCOMPLETED_UNCLAIMED_ROW_QUERY.format(
            tbl_name=self.jobset_id))

    def get_oldest_uncompleted_job(self, max_n_retry_attempts):
        params = {'max_n_retry_attempts': max_n_retry_attempts}
        return self.db_handle.one(OLDEST_UNCOMPLETED_ROW_QUERY.format(
            tbl_name=self.jobset_id), parameters=params)

    def update_job_n_failed_attempts(self, job_id):
        self.db_handle.run(UPDATE_ROW_N_FAILED_ATTEMPTS_BY_ID_QUERY.format(
            tbl_name=self.jobset_id), parameters={'id': job_id})

    def set_job_as_claimed(self, job_id):
        self.db_handle.run(SET_ROW_CLAIMED_BY_ID_QUERY.format(
            tbl_name=self.jobset_id), parameters={'id': job_id})

    def run(self, job_callable, busywait=False,
            max_busywait_sleep=None, max_failure_sleep=None,
            max_n_retry_attempts=10):
        busywait_decay = exponential_decay(max_value=max_busywait_sleep)
        fail_decay = exponential_decay(max_value=max_failure_sleep)

        try:
            while True:
                a_row = self.get_uncompleted_unclaimed_job()
                if a_row is None:
                    # there is nothing left that is unclaimed, so we may as well
                    # 'repeat' already claimed work - maybe we can beat another
                    # worker to complete it.
                    self.logger.info('No unclaimed work remains - re-claiming '
                                     'oldest job')
                    a_row = self.get_oldest_uncompleted_job(max_n_retry_attempts)

                if a_row is None:
                    if busywait:
                        d = next(busywait_decay)
                        self.logger.info('No uncompleted work - busywait with '
                                         'binary exponential decay '
                                         '({} seconds)'.format(d))
                        time.sleep(d)
                    else:
                        raise JobsExhaustedError()
                else:
                    # Reset the busywait decay
                    busywait_decay = exponential_decay(
                        max_value=max_busywait_sleep)

                    # let's claim the job
                    self.set_job_as_claimed(a_row.id)

                    self.logger.info('Claimed job (id: {})'.format(a_row.id))
                    try:
                        with timer() as t:
                            output_data = job_callable(a_row.input_data)
                    except JobFailed:
                        d = next(fail_decay)
                        self.update_job_n_failed_attempts(a_row.id)
                        self.logger.warn('Failed to complete job (id: {}) - '
                                         'sleeping with binary exponential '
                                         'decay ({}s)'.format(a_row.id, d))
                        time.sleep(d)
                    else:
                        # Reset the failure decay
                        fail_decay = exponential_decay(
                            max_value=max_failure_sleep)

                        # Update the job information
                        self.set_job_as_complete(a_row.id, t.interval,
                                                 output_data=output_data)
                        total_secs = t.interval.total_seconds()
                        self.logger.info('Completed job (id: {}) in {:.2f} '
                                         'seconds'.format(a_row.id, total_secs))
        except JobsExhaustedError:
            self.logger.info('All jobs are exhausted, terminating.')

    def _stats_report(self):
        return get_stats_report(self.db_handle, self.jobset_id)

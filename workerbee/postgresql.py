from __future__ import division
import os
import sys
import warnings

from postgres import Postgres
from psycopg2.extras import Json as postgres_jsonify

from .base import JobsExhaustedError, JobFailed

###############################################################################

TABLE_EXISTS_QUERY = r"""
SELECT EXISTS(
    SELECT *
    FROM information_schema.tables
    WHERE table_name = 'job_{}'
);
"""

CREATE_TABLE_QUERY = r"""
CREATE TABLE job_{}(
  job_id TEXT UNIQUE NOT NULL PRIMARY KEY,
  data json,
  completed BOOLEAN NOT NULL DEFAULT FALSE,
  claimed BOOLEAN NOT NULL DEFAULT FALSE
)
"""

INSERT_JOB_QUERY = r"""
INSERT INTO job_{0} (job_id) VALUES (%(job_id)s) RETURNING job_id
"""

UPDATE_DATA_QUERY = r"""
UPDATE job_{0} SET data=%(data)s WHERE job_id=%(job_id)s
"""

RANDOM_UNCOMPLETED_UNCLAIMED_ROW_QUERY = r"""
SELECT *
FROM job_{0} WHERE completed=false AND claimed=false OFFSET floor(random() * (
    SELECT COUNT(*) FROM job_{0} WHERE completed=false AND claimed=false
))
LIMIT 1;
"""

RANDOM_UNCOMPLETED_ROW_QUERY = r"""
SELECT *
FROM job_{0} WHERE completed=false OFFSET floor(random() * (
    SELECT COUNT(*) FROM job_{0} WHERE completed=false
))
LIMIT 1;
"""

SET_ROW_COMPLETED_BY_ID_QUERY = r"""
UPDATE job_{} SET completed=true WHERE job_id=%(job_id)s
"""

SET_ROW_CLAIMED_BY_ID_QUERY = r"""
UPDATE job_{} SET claimed=true WHERE job_id=%(job_id)s
"""

TOTAL_ROWS_QUERY = r"""
SELECT COUNT(*) FROM job_{0}
"""

COMPLETED_ROWS_QUERY = r"""
SELECT COUNT(*) FROM job_{0} WHERE completed=true
"""


###############################################################################
def table_exists(db_handle, table_id):
    return db_handle.one(TABLE_EXISTS_QUERY.format(table_id))


def setup_table(db_handle, table_id, job_ids, job_dicts=None):
    does_table_exist = table_exists(db_handle, table_id)

    if job_dicts is None:
        job_dicts = {}

    if not does_table_exist:
        print('Creating table...')
        with db_handle.get_cursor() as cursor:  # Single Transaction
            # Create table
            cursor.run(CREATE_TABLE_QUERY.format(table_id))

            # Fill in list of jobs
            for job_id in job_ids:
                cursor.one(INSERT_JOB_QUERY.format(table_id),
                           parameters={'job_id': job_id})
                if job_id in job_dicts:
                    cursor.run(UPDATE_DATA_QUERY.format(table_id),
                               parameters={'data': postgres_jsonify(job_dicts[job_id]),
                                           'job_id': job_id})
    return not does_table_exist


def create_db_handle(host=None, database=None, user=None):
    if host is None:
        host = os.environ['PGHOST']
    if database is None:
        database = os.environ['PGDATABASE']
    if user is None:
        user = os.environ['PGUSER']
    return Postgres("host={} dbname={} user={}".format(host, database, user))


def get_random_uncompleted_unclaimed_job(db_handle, table_id):
    return db_handle.one(RANDOM_UNCOMPLETED_UNCLAIMED_ROW_QUERY.format(table_id))


def get_random_uncompleted_job(db_handle, table_id):
    return db_handle.one(RANDOM_UNCOMPLETED_ROW_QUERY.format(table_id))


def set_job_as_complete(db_handle, table_id, job_id):
    db_handle.run(SET_ROW_COMPLETED_BY_ID_QUERY.format(table_id),
                  parameters={'job_id': job_id})


def set_job_as_claimed(db_handle, table_id, job_id):
    db_handle.run(SET_ROW_CLAIMED_BY_ID_QUERY.format(table_id),
                  parameters={'job_id': job_id})


def get_total_job_count(db_handle, table_id):
    return db_handle.one(TOTAL_ROWS_QUERY.format(table_id))


def get_completed_job_count(db_handle, table_id):
    return db_handle.one(COMPLETED_ROWS_QUERY.format(table_id))

################################################################################


def postgres_exhaust_all(perform_job_f, table_id, job_ids=None, job_dicts=None,
                         host=None, database=None, user=None, verbose=True):
    db_handle = create_db_handle(host=host, database=database, user=user)
    if job_ids is not None:
        if not setup_table(db_handle, table_id, job_ids, job_dicts=job_dicts):
            warnings.warn('The input table name "{}" already existed and the '
                          'newly provided job IDs were not used.'.format(table_id))
    else:
        if not table_exists(db_handle, table_id):
            raise ValueError('Job IDs were not provided and the given '
                             'table ID does not exist.')
    n_inputs = get_total_job_count(db_handle, table_id)
    if verbose:
        i = 0
    try:
        while True:
            if verbose:
                sys.stdout.write(str(i) + ': ')
                sys.stdout.flush()
            a_row = get_random_uncompleted_unclaimed_job(db_handle, table_id)
            if a_row is None:
                # there is nothing left that is unclaimed, so we may as well
                # 'repeat' already claimed work - maybe we can beat another
                # bee to complete it.
                a_row = get_random_uncompleted_job(db_handle, table_id)
                if verbose:
                    sys.stdout.write("no unclaimed work remains - ")
                    sys.stdout.flush()

            if a_row is None:
                raise JobsExhaustedError()

            # let's claim the job
            set_job_as_claimed(db_handle, table_id, a_row.job_id)

            if verbose:
                sys.stdout.write("claimed '{}'".format(a_row.job_id))
                sys.stdout.flush()
            try:
                perform_job_f(a_row.job_id, data=a_row.data)
            except JobFailed as e:
                sys.stdout.write('FAILED.\n')
                sys.stdout.flush()
            else:
                set_job_as_complete(db_handle, table_id, a_row.job_id)
                if verbose:
                    sys.stdout.write('...done.\n')
                    sys.stdout.flush()
            if verbose:
                i += 1
                if i % 10 == 0:
                    n_completed_rows = get_completed_job_count(db_handle,
                                                               table_id)
                    print('{:.2f}% ({}/{}) Completed'.format(
                        (n_completed_rows / n_inputs) * 100,
                        n_completed_rows, n_inputs))
    except JobsExhaustedError:
        sys.stdout.write('\rJobs exhausted')
        sys.stdout.flush()

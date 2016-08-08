from __future__ import division
import os
import sys

from postgres import Postgres
from psycopg2.extras import Json as postgres_jsonify

from .base import JobsExhaustedError, JobFailed

from string import ascii_letters, digits
ALLOWED_CHARACTERS_IN_TABLE_NAME = set(letters.lower()) | set(digits) | set('_')


def check_name(table_name):
    if not isinstance(table_name, basestring):
        raise TypeError("Experiment ID '{}' is of type {}, not string".format(
            table_name, type(table_name)))
    invalid = set(table_name) - ALLOWED_CHARACTERS_IN_TABLE_NAME
    if len(invalid) > 0:
        raise ValueError("Invalid characters in experiment ID: {} "
                         "(allowed [a...z, 0...9, '_']".format(
            ', '.join(["'{}'".format(l) for l in sorted(list(invalid))])))


###############################################################################

TABLE_EXISTS_QUERY = r"""
SELECT EXISTS(
    SELECT *
    FROM information_schema.tables
    WHERE table_name = 'wb__{}'
);
"""

CREATE_TABLE_QUERY = r"""
CREATE TABLE wb__{}(
  job_id TEXT UNIQUE NOT NULL PRIMARY KEY,
  data json,
  completed BOOLEAN NOT NULL DEFAULT FALSE,
  claimed BOOLEAN NOT NULL DEFAULT FALSE
)
"""

INSERT_JOB_QUERY = r"""
INSERT INTO wb__{0} (job_id) VALUES (%(job_id)s) RETURNING job_id
"""

UPDATE_DATA_QUERY = r"""
UPDATE wb__{0} SET data=%(data)s WHERE job_id=%(job_id)s
"""

RANDOM_UNCOMPLETED_UNCLAIMED_ROW_QUERY = r"""
SELECT *
FROM wb__{0} WHERE completed=false AND claimed=false OFFSET floor(random() * (
    SELECT COUNT(*) FROM wb__{0} WHERE completed=false AND claimed=false
))
LIMIT 1;
"""

RANDOM_UNCOMPLETED_ROW_QUERY = r"""
SELECT *
FROM wb__{0} WHERE completed=false OFFSET floor(random() * (
    SELECT COUNT(*) FROM wb__{0} WHERE completed=false
))
LIMIT 1;
"""

SET_ROW_COMPLETED_BY_ID_QUERY = r"""
UPDATE wb__{} SET completed=true WHERE job_id=%(job_id)s
"""

SET_ROW_CLAIMED_BY_ID_QUERY = r"""
UPDATE wb__{} SET claimed=true WHERE job_id=%(job_id)s
"""

TOTAL_ROWS_QUERY = r"""
SELECT COUNT(*) FROM wb__{0}
"""

COMPLETED_ROWS_QUERY = r"""
SELECT COUNT(*) FROM wb__{0} WHERE completed=true
"""


###############################################################################
def table_exists(db_handle, table_id):
    return db_handle.one(TABLE_EXISTS_QUERY.format(table_id))


def setup_table(db_handle, experiment_id, job_ids=None, job_data=None,
                verbose=False):
    check_name(experiment_id)
    does_table_exist = table_exists(db_handle, experiment_id)

    if job_data is None:
        job_data = {}
    if job_ids is not None:
        extra_job_data = set(job_data) - set(job_ids)
        if len(extra_job_data) > 0:
            raise ValueError("job_data provided for {} job IDs that aren't in this "
                             "experiment: {}".format(len(extra_job_data),
                                                     list(extra_job_data)))
    if does_table_exist:
        if verbose:
            print("Table already exists for experiment '{}'".format(experiment_id))
            if job_ids is not None:
                print('Warning: {} provided job IDs were ignored (presumably '
                      'they are already in the table?)'.format(len(job_ids)))
    else:
        if verbose:
            print("Creating table for experiment '{}'...".format(experiment_id))
        with db_handle.get_cursor() as cursor:  # Single Transaction
            # Create table
            cursor.run(CREATE_TABLE_QUERY.format(experiment_id))
            if verbose:
                print("Adding {} jobs to experiment "
                      "'{}'...".format(len(job_ids), experiment_id))
                if verbose:
                    if len(job_data) > 0:
                        print('job_data provided for {} jobs'.format(len(job_data)))
                    else:
                        print('no job_data provided.')
            # Fill in list of jobs
            for job_id in job_ids:
                cursor.one(INSERT_JOB_QUERY.format(experiment_id),
                           parameters={'job_id': job_id})
                if job_id in job_data:
                    cursor.run(UPDATE_DATA_QUERY.format(experiment_id),
                               parameters={'data': postgres_jsonify(job_data[job_id]),
                                           'job_id': job_id})
        if verbose:
            print("Experiment '{}' set up.".format(experiment_id))
    if verbose:
        print('-' * 80)
    return not does_table_exist


def create_db_handle(host=None, port=None, user=None, database=None, verbose=False):
    if host is None:
        host = os.environ['PGHOST']
    if port is None:
        port = os.environ['PGPORT']
    if user is None:
        user = os.environ['PGUSER']
    if database is None:
        database = os.environ['PGDATABASE']
    if verbose:
        print('Connecting to database {} on {}@{}:{}...'.format(database, user, host, port))
        if 'PGPASS' in os.environ:
            print(' - Password is set via environment variable PGPASS.')
        else:
            print(' - No password is set. (If needed, set the environment '
                  'variable PGPASS.)')
    return Postgres("host={} port={} user={} dbname={} ".format(host, port, user, database))


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


def postgres_experiment(experiment_id, job_function,
                        job_ids=None, job_data=None,
                        host=None, port=None, user=None, database=None,
                        verbose=True):
    """

    Parameters
    ----------
    experiment_id : `str`
        A unique identifier for an experiment. Must be a valid Postgres Table
        name (no whitespace etc).
    job_function : `callable` taking (job_id: str, job_data : {})
        A callable that performs a unit of work in this experiment. The
        function will be provided with two arguments - the ``job_id`` for this
        job (a string) and the data payload for this job (a dictionary). This
        function then uses these inputs to perform the relevent work for the
        experiment. If the function completes without error, the job will be
        automatically marked complete in the database.
    job_ids : `list` of `str`, optional if experiment_id exists.
        A list of Job IDs to be used in this experiment. If the experiment
        already exists, these jobs will be ignored. If the experiment ID
        doesn't exist yet, the experiment will be created with these jobs.
    job_data : `dict` with `str` keys pointing to `dict`s, optional
        An optional dictionary of extra data that can be stored per-job. This
        dictionary's top-level keys should be job_id's. top-level values
        should be dictionaries of basic datatypes. This dictionary can for
        example store experiment parameters that should be used for the job
        in question.
    host : `str`, optional
        The hostname of the Postgres instance. If ``None``, the environment
        variable ``PGHOST`` will be used if set.
    port : `str`, optional
        The port which the Postgres instance is running on. If ``None``, the
        environment variable ``PGPORT`` will be used if set.
    user : `str`, optional
        The Postgres user to join as. If ``None``, the
        environment variable ``PGUSER`` will be used if set.
    database : `str`, optional
        The database of the Postgres server to use. If ``None``, the
        environment variable ``PGDATABASE`` will be used if set.
    verbose : `bool`, optional
        If ``True``, additional logging will be provided.
    """
    db_handle = create_db_handle(host=host, port=port, user=user,
                                 database=database, verbose=verbose)
    if job_ids is not None:
        setup_table(db_handle, experiment_id, job_ids=job_ids,
                    job_data=job_data, verbose=verbose)
    else:
        if not table_exists(db_handle, experiment_id):
            raise ValueError('Job IDs were not provided and the given '
                             'Experiment ID "{}" does not exist.'.format(experiment_id))
    n_inputs = get_total_job_count(db_handle, experiment_id)
    if verbose:
        i = 0
    try:
        while True:
            if verbose:
                sys.stdout.write(str(i) + ': ')
                sys.stdout.flush()
            a_row = get_random_uncompleted_unclaimed_job(db_handle, experiment_id)
            if a_row is None:
                # there is nothing left that is unclaimed, so we may as well
                # 'repeat' already claimed work - maybe we can beat another
                # bee to complete it.
                a_row = get_random_uncompleted_job(db_handle, experiment_id)
                if verbose:
                    sys.stdout.write("no unclaimed work remains - ")
                    sys.stdout.flush()

            if a_row is None:
                raise JobsExhaustedError()

            # let's claim the job
            set_job_as_claimed(db_handle, experiment_id, a_row.job_id)

            if verbose:
                sys.stdout.write("claimed '{}'".format(a_row.job_id))
                sys.stdout.flush()
            try:
                job_function(a_row.job_id, a_row.data if a_row.data is not None else {})
            except JobFailed as e:
                sys.stdout.write('FAILED.\n')
                sys.stdout.flush()
            else:
                set_job_as_complete(db_handle, experiment_id, a_row.job_id)
                if verbose:
                    sys.stdout.write('...done.\n')
                    sys.stdout.flush()
            if verbose:
                i += 1
                if i % 10 == 0:
                    n_completed_rows = get_completed_job_count(db_handle,
                                                               experiment_id)
                    print('{:.2f}% ({}/{}) Completed'.format(
                        (n_completed_rows / n_inputs) * 100,
                        n_completed_rows, n_inputs))
    except JobsExhaustedError:
        sys.stdout.write('\rAll jobs are exhausted, terminating.')
        sys.stdout.flush()

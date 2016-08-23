# workerbee
*A simple decentralised framework for fault tolerant distributable jobs*

**Workerbee** is a simple framework that makes it easy to coordinate and run
highly dsitributable sets of jobs over computing clusters. Workerbee requires:

1. A Python function you need to evaluate against many unique inputs, or
   **jobs**. Each job takes as input any JSON-serializable Python dict.
2. Some mechanism to start many instances of a Python script. That might be as
   simple as pre-existing cluster management system to spin up many instances
   of your script (e.g. a [HTCondor](https://research.cs.wisc.edu/htcondor/) setup)
3. A [PostgreSQL](https://www.postgresql.org) database that all worker instances
   can see (e.g. an instance) with version >= 9.2.

Each set (collection of unique inputs) of jobs that Workerbee handles is called
a **JobSet**. Each jobset is given a unique **`jobset_id`**.
Each jobset contains a number of unique **jobs**.

- `jobset_id` can use lowercase ASCII characters, numbers, and underscores.
  An example jobset ID may be `texturemap_2016_08_24`.
- `input_data` must be unique across a jobset. This is to avoid unnecessary work
  by the workers.
- A job is assumed to be both deterministic and idempotent. In order to attempt
  to maximise throughput, slow running jobs may be completed multiple times
  by idle workers in order to attempt to be fault tolerant.

Workerbee is a Python framework that you run **on every instance of your process
on all machines**. That is to say, you modify your processing script to look
something like:
```py
from workerbee import JobSet
...

def job_function(input_data):
    ...

jobset = JobSet('a_valid_id_00')
# Below we assume setup_jobset and add_jobs have already been called.
jobset.run(job_function)
```
Note that each script you run is the same - there is no requirement for a
'master' script that orchestrates behavior - the key principle here is each
'workerbee' independently decides what is the best next job to run to complete
the jobset as fast as possible.

A shared database is only used to store a minimal amount of data to run the
jobset - in particular the `jobset_id` and `input_data`s. The `input_data` is
expected to be a JSON serializable Python `dict` that specifies the information
necessary to complete a job. Each worker may, optionally, return a JSON
serializable Python `dict` in order to store the results of a job.

All bees fall into a pattern of claiming a random uncompleted job from the
current jobset to work on. You provide the workerbee jobset with a
`job_callable` - this callable/function will be invoked, passing in the
`input_data` for the job. If your `job_function` returns without error, the job
will be marked `completed` in the database. If your job cannot be completed by
the worker, a ``JobFailedError`` exception should be raised and the
job will be retried by another worker after all other unclaimed work is
completed.

The jobset continues until all work is completed, at which point each worker
independently comes to this realization and terminates. Alternatively, a worker
may busy wait until new jobs are added to the jobset. In order to reduce
load on the database, the busywaiting is implemented as a `sleep` with a binary
exponential backoff.

## Practical Usage
For now the only database supported for workerbee is PostgreSQL >= 9.2. Due to
the use of the JSON type for data storage, PostgreSQL >= 9.4 is advised.

### Running on a single node
Here we assume the most basic case where you have a set of jobs to do (which
could easily be completed via a simple for loop!) and we will run the code below
on a single machine.

```py
import time
from workerbee import JobSet


def job_function(job_data):
    print('Processing job with data: {}'.format(job_data))
    time.sleep(1)


jobset = JobSet('a_valid_id_01',
                host='localhost', port='5432',
                user='postgres', dbname='postgres')
# Create the jobset table within PostgreSQL - can only be called once
jobset.setup_jobset()  
# These data dicts must be unique. 
# If the 'id' key is used, it must also be unique
jobset.add_jobs([{'id': 0, 'data': 'test'},
                 {'id': 1, 'data': 'test2'}])
jobset.run(job_function)
```
The above script will run until all jobs have been processed by the worker. If
the script is run twice it will fail due to trying to recreate an already 
existing jobset.

Key takeaways:

1. To use workerbee you need to form a function with the signature:
    ```py
    def job_function(job_data):
        ...
    ```
2. `job_data` must be unique within the jobset. If you are using
   PostgreSQL < 9.4 you may enforce this via a reserved key in the `input_data`
   dictionary `id`. All `id` fields must be unique across the entire jobset.
3. Workerbee uses the function to process a jobset. Your function will be
   called with available work to be done. A successful exit of your function
   means this unit of work will not be attempted again.
4. For this example, no data is persisted about the result of the job, other
   than the assumption that it completed successfully.

### Running on many nodes
Below we assume the script will be run on many machines. How this code is
deployed is not discussed here, but we recommend batch processing systems
such as [HTCondor](https://research.cs.wisc.edu/htcondor/).

```py
import time
from workerbee import JobSet


def job_function(job_data):
    print('Processing job with data: {}'.format(job_data))
    time.sleep(1)
    
jobset = JobSet('a_valid_id_02',
                host='localhost', port='5432',
                user='postgres', dbname='postgres')
# Create the jobset table within PostgreSQL - can only be called once
if jobset.setup_jobset(ignore_existing_jobset=True):
  # These data dicts must be unique. 
  # The 'id' doesn't have to be used, but each dict must be unique
  jobset.add_jobs([{'data': 'test'},
                   {'data': 'test2'}])
jobset.run(job_function)
```

In contrast to the previous example, we ignore the error that would otherwise
be thrown from trying to recreate the jobset and only add the jobs on the node
that succeeded in creating the jobset table. This provides a script that is
tolerant to running on many nodes with no master node being required to 
setup the database.

### Logging
Workerbee supports logging via the built-in Python logging framework. Workerbee
provides a default logger ('workerbee') which prints a formatted message to
`stdout`. It may be preferable to implement your own logger with a custom
handler that logs to a file for example.

```py
import logging
import time
import sys
from workerbee import JobSet, JobFailedError


# Create a logger that only logs warning messages to stderr
custom_logger = logging.getLogger('stderr_warnings_only')
# Clear default handlers
custom_logger.handlers = []
custom_logger.addHandler(logging.StreamHandler(sys.stderr))
custom_logger.handlers[-1].setFormatter(
  logging.Formatter('(%(asctime)-5s): %(message)s'))
custom_logger.handlers[-1].setLevel(logging.WARNING)


force_a_warning = True
def job_function(job_data):
    global force_a_warning
    if force_a_warning:
        force_a_warning = False
        raise JobFailedError()
    print('Processing job with data: {}'.format(job_data))
    time.sleep(1)


jobset = JobSet('a_valid_id_03', logger_name='stderr_warnings_only',
                host='localhost', port='5432',
                user='postgres', dbname='postgres')
# Create the jobset table within PostgreSQL - can only be called once
if jobset.setup_jobset(ignore_existing_jobset=True):
  # These data dicts must be unique.
  # If the 'id' key is used, it must also be unique
  jobset.add_jobs([{'id': 0, 'data': 'test'},
                   {'id': 1, 'data': 'test2'}])
jobset.run(job_function)
```

### Handling failures
In certain cases, it might be expected that it is possible for a job to fail
due to an unpredictable but possibly recoverable error. For example, a temporary
disconnection of a remote file server. In this case, you may want to guard
against these exceptions and raise the `JobFailedError` exception.
In contrast to other exceptions, `JobFailedError` is tracked by the jobset
and workers are tolerant to job failures up to a maximum number of retries. This
is very useful for guarding against corrupted nodes. Note that all uncaught
exceptions will cause the workerbee process to terminate as normal.

For example, below we simulate an `OSError` by creating a read-only file and
attempting to write into it.
```py
import time
from pathlib import Path
from workerbee import JobSet, JobFailedError


# Create a read only file that will raise a PermissionDeniedError
READONLY_TMP_FILE = Path('/tmp/0')
if READONLY_TMP_FILE.exists():
    READONLY_TMP_FILE.unlink()
READONLY_TMP_FILE.touch(mode=0o400)


def job_function(job_data):
    try:
        Path('/tmp/{}'.format(job_data['id'])).write_text('test')
        time.sleep(1)
    except OSError:
        raise JobFailedError()


jobset = JobSet('a_valid_id_04',
                host='localhost', port='5432',
                user='postgres', dbname='postgres')
# Create the jobset table within PostgreSQL - can only be called once
if jobset.setup_jobset(ignore_existing_jobset=True):
    # These data dicts must be unique.
    # If the 'id' key is used, it must also be unique
    jobset.add_jobs([{'id': 0, 'data': 'test'},
                     {'id': 1, 'data': 'test2'}])
jobset.run(job_function, max_n_retry_attempts=3)
```
Note that this jobset is thus not fully completed as the job with `id=0` cannot
complete, though it is attempted 3 times by workerbee. After every failure the
worker waits for an increasing period
([binary exponential backoff](https://en.wikipedia.org/wiki/Exponential_backoff))
to prevent unnecessary database load.

Alternatively, you may want to be resilient to all errors, this would be easily
achieved with a catch all `try`/`except`:
```py
def job_function(job_data):
    try:
        ...
    except:
        raise JobFailedError()
```
Which will never terminate due to an exception. To avoid boilerplate, we provide
a decorator that provides this functionality.
```py
from workerbee import catch_all_exceptions

@catch_all_exceptions
def job_function(job_data):
    # ALL exceptions are re-raised as JobFailedError
    ...
```
**Be cautuous with this paradigm - as some nodes may enter an irrecoverable
state that will mark all jobs as failures!**.
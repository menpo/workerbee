workerbee
=========

*A simple decentralised framework for embarrassingly distributable jobs*

**Workerbee** is a simple framework that makes it easy to coordinate and run highly
parallelizable jobsets over computing clusters. Workerbee requires:

1. A Python function you need to evaluate against many inputs, or **jobs**. Each job takes as input any JSON-serializable Python dict.
2. Some mechnism to start many insances of a Python script. That might be as simple as pre-existing cluster management system to spin up many instances of your script (e.g. a [HTCondor](https://research.cs.wisc.edu/htcondor/) setup)
3. A [PostgreSQL](https://www.postgresql.org) database that all worker instances can see (e.g. a  instance)

Each collection of jobs that Workerbee handles is called a **jobset**. Each jobset is given a unique **`jobset_id`**. 
Each jobset contains a number of **jobs**.

- `jobset_id`'s can use lowercase letters, numbers, and underscores. A good choice may be `texturemap_2016_08_24`.
- `job_id`s can be any string, so long as it is unique for every job in the jobset. 
  A good choice for an operation on many images could be the path to each image, e.g. `/vol/atlas/lenna.png`

Workerbee is a Python framework that you run **on every instance of your process on all machines**. That is to say,
you modify your processing script to look something like:
```py
from workerbee.postgres import postgres_jobset
...

def my_job(job_id, job_data):
    ...

postgres_jobset('texturemap_2016_08_24', my_job, ...)
```
Note that each script you run is the same - there is no 'master' script that orchestrates behavior - the key principle
here is each 'workerbee' independently decides what is the best next job to run to complete the jobset as fast as
possible.

A shared database is only used to store a minimal amount of data to run the
jobset - in particular the `jobset_id` and `job_id`s. You are also permitted to store an 
arbitrary dictionary of extra data per job - this can be useful for storing the parameters for your 
jobset for instance.

Each worker independently tries to setup the jobset by creating the necessary tables in the database. 
One will succeed - other's will fail but this just means another worker got there first. All bees then
fall into a pattern of claiming a random `job_id` from the current jobset to work on. You provide the workerbee
setup function with a `job_function` - this function will be invoked, passing in the current `job_id` and any associated
`job_data` stored for this job in the database. If your `job_function` returns without error, the job will be marked `COMPLETE`
in the database. If your job errors, the job will be retried by another worker after all other unclaimed work is done.

The jobset continues until all work is completed, at which point each worker independently comes to this realization
and terminates.

Usage with Postgres
-------------------

For now the only database supported for workerbee is postgres. Here is a complete example to get you started:

```py
from workerbee.postgresql import postgres_jobset

from time import sleep

def my_job(job_id, job_data):
    print('\n\n - processing job {} with data: {}\n\n'.format(job_id, job_data))
    sleep(1)
    
postgres_jobset('texturemap_2016_08_25', my_job, 
                    job_ids=['id1', 'id2', 'id3'], 
                    job_data={ 'id1': {'some': 2, 'data': 'here'}}, 
                    host='localhost', port='5432', 
                    user='postgres', database='postgres', verbose=True)
```
Output:
```
Connecting to database postgres on postgres@localhost:5432...
 - No password is set. (If needed, set the environment variable PGPASS.)
Creating table for jobset 'texturemap_2016_08_25'...
Adding 3 jobs to jobset 'texturemap_2016_08_25'...
job_data provided for 1 jobs
jobset 'texturemap_2016_08_25' set up.
--------------------------------------------------------------------------------
0: claimed 'id2'

 - processing job id2 with data: {}


...done.
1: claimed 'id3'

 - processing job id3 with data: {}


...done.
2: claimed 'id1'

 - processing job id1 with data: {u'some': 2, u'data': u'here'}


...done.
All jobs are exhausted, terminating.
```
Key takeaways:

1. To use workerbee you need to form a function with the signature:
```py
def job_function(job_id, job_data):
    ...
```
2. `job_id` is a lightweight string that uniquely identifies a job. If you need expensive resources for your job, 
e.g. loading a large asset for processing, you should do this inside your job function. In such cases, paths make a great
choice for `job_id`.
3. Workerbee wraps this function in an jobset. Your function will be called with available work to be done. The successful exit of your function means this unit of work will not be done again
4. Results are not saved by workerbee into the database or anywhere else. You should persist results however makes sense for you without your scripts.

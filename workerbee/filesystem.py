from functools import partial
from .base import exhaust_all, random_next_job
from pathlib import Path


def as_paths(p):
    return [Path(i) for i in p]


def filestem_difference(todo, done):
    paths_todo, paths_done = as_paths(todo), as_paths(done)
    todo_stem_to_path = {t.stem: t for t in paths_todo}
    if len(todo_stem_to_path) != len(paths_todo):
        raise ValueError('todo paths are not uniquely identified by stems')
    remaining_stems = set(todo_stem_to_path) - set(d.stem for d in paths_done)
    return [todo_stem_to_path[i] for i in remaining_stems]


random_next_file_from_stems = partial(random_next_job, filestem_difference)
exhaust_all_files_randomly = partial(exhaust_all, random_next_file_from_stems)

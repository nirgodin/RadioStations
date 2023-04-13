import os
from typing import List

from git import Repo


def commit_and_push(files_paths: List[str], commit_message: str, branch_name: str = 'main') -> None:
    repo = Repo(os.getcwd())
    index = repo.index
    index.add(files_paths)
    index.commit(commit_message)
    merge_message = f'Automatically merged the following files {", ".join(files_paths)}'
    repo.git.merge('--no-ff', '-m', merge_message, branch_name)
    origin = repo.remote()
    origin.push()

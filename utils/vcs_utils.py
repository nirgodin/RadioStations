import os
import subprocess
from typing import List

from git import Repo

from consts.path_consts import MAIN_DATA_DIR_PATH, DATA_DVC_PATH


def commit_and_push(files_paths: List[str], commit_message: str, branch_name: str = 'main') -> None:
    repo = Repo(os.getcwd())
    index = repo.index
    index.add(files_paths)
    index.commit(commit_message)
    merge_message = f'Automatically merged the following files {", ".join(files_paths)}'
    repo.git.merge('--no-ff', '-m', merge_message, branch_name)
    origin = repo.remote()
    origin.push()


def dvc_add_and_push(commit_message: str, branch_name: str = 'main') -> None:
    subprocess.call(['dvc', 'add', '-v', MAIN_DATA_DIR_PATH])
    commit_and_push(
        files_paths=[DATA_DVC_PATH],
        commit_message=commit_message,
        branch_name=branch_name
    )
    subprocess.call(['dvc', '-v', 'push'])

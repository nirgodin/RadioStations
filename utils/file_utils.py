import json
import os
from pathlib import Path
from typing import Union, List, Optional

from pandas import DataFrame

from consts.miscellaneous_consts import JSON_ENCODING, UTF_8_ENCODING
from tools.google_drive.google_drive_adapter import GoogleDriveAdapter
from tools.google_drive.google_drive_file_metadata import GoogleDriveFileMetadata
from utils.general_utils import is_remote_run


def to_json(d: Union[dict, list], path: str) -> None:
    with open(path, 'w', encoding=JSON_ENCODING) as f:
        json.dump(d, f, ensure_ascii=False, indent=4)


def read_json(path: str) -> dict:
    with open(path, 'r', encoding=JSON_ENCODING) as f:
        return json.load(f)


def to_csv(data: DataFrame, output_path: str, header: bool = True, mode: str = 'w') -> None:
    dir_path = Path(os.path.dirname(output_path))

    if not os.path.exists(dir_path):  # For remote runs
        dir_path.mkdir(parents=True)

    data.to_csv(output_path, index=False, encoding=UTF_8_ENCODING, header=header, mode=mode)


def append_to_csv(data: DataFrame, output_path: str) -> None:
    if os.path.exists(output_path):
        to_csv(data=data, output_path=output_path, header=False, mode='a')
    else:
        to_csv(data=data, output_path=output_path)


def load_txt_file_lines(path: str) -> List[str]:
    with open(path, encoding=JSON_ENCODING) as f:
        hebrew_words: str = f.read()

    return hebrew_words.split('\n')


def upload_files_to_drive(*files_metadata: GoogleDriveFileMetadata) -> None:
    if is_remote_run():
        GoogleDriveAdapter().upload(files_metadata)

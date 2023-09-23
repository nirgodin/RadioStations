import json
import os
from pathlib import Path
from typing import Union, List

from pandas import DataFrame

from consts.miscellaneous_consts import JSON_ENCODING, UTF_8_ENCODING
from tools.csv_appender import CSVAppender


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
        CSVAppender.append(data=data, output_path=output_path)
    else:
        to_csv(data=data, output_path=output_path)


def load_txt_file_lines(path: str) -> List[str]:
    with open(path, encoding=JSON_ENCODING) as f:
        hebrew_words: str = f.read()

    return hebrew_words.split('\n')


def get_path_suffix(path: str) -> str:
    return Path(path).suffix


def append_dict_to_json(existing_data: dict, new_data: dict, path: str) -> None:
    new_data.update(existing_data)
    to_json(new_data, path)

import json
import os
from pathlib import Path
from typing import Union, List

from pandas import DataFrame

from consts.miscellaneous_consts import JSON_ENCODING, UTF_8_ENCODING


def to_json(d: Union[dict, list], path: str) -> None:
    with open(path, 'w', encoding=JSON_ENCODING) as f:
        json.dump(d, f, ensure_ascii=False, indent=4)


def read_json(path: str) -> dict:
    with open(path, 'r', encoding=JSON_ENCODING) as f:
        return json.load(f)


def to_csv(data: DataFrame, output_path: str, header: bool = True, mode: str = 'w') -> None:
    if not os.path.exists(output_path):  # For remote runs
        dir_path = Path(os.path.dirname(output_path))
        dir_path.mkdir(parents=True)

    data.to_csv(output_path, index=False, encoding=UTF_8_ENCODING, header=header, mode=mode)


def append_to_csv(data: DataFrame, output_path: str) -> None:
    if os.path.exists(output_path):
        to_csv(data=data, output_path=output_path, header=False, mode='a')
    else:
        to_csv(data=data, output_path=output_path)


def load_txt_file_lines(path: str) -> List[str]:
    with open(path, encoding='utf-8') as f:
        hebrew_words: str = f.read()

    return hebrew_words.split('\n')
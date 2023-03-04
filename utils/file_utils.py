import json
import os
from typing import Union, List

from pandas import DataFrame

from consts.miscellaneous_consts import JSON_ENCODING, UTF_8_ENCODING


def to_json(d: Union[dict, list], path: str) -> None:
    with open(path, 'w', encoding=JSON_ENCODING) as f:
        json.dump(d, f, ensure_ascii=False, indent=4)


def read_json(path: str) -> dict:
    with open(path, 'r', encoding=JSON_ENCODING) as f:
        return json.load(f)


def append_to_csv(data: DataFrame, output_path: str) -> None:
    if os.path.exists(output_path):
        data.to_csv(output_path, header=False, index=False, mode='a', encoding=UTF_8_ENCODING)
    else:
        data.to_csv(output_path, index=False, encoding=UTF_8_ENCODING)


def load_txt_file_lines(path: str) -> List[str]:
    with open(path, encoding='utf-8') as f:
        hebrew_words: str = f.read()

    return hebrew_words.split('\n')
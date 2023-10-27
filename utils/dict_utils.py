from typing import Optional


def merge_dicts(*dicts: Optional[dict]) -> dict:
    merged_dict = {}

    for d in dicts:
        if isinstance(d, dict):
            merged_dict.update(d)

    return merged_dict


def sort_dict_by_descending_value(dct: dict) -> dict:
    return dict(sorted(dct.items(), key=lambda x: x[1], reverse=True))

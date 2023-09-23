from typing import Optional


def merge_dicts(*dicts: Optional[dict]) -> dict:
    merged_dict = {}

    for d in dicts:
        if isinstance(d, dict):
            merged_dict.update(d)

    return merged_dict

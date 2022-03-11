import re
from typing import List


def get_words(path: str) -> List[str]:
    with open(path, encoding='utf-8') as f:
        hebrew_words: str = f.read()

    return hebrew_words.split('\n')


def contains_any_relevant_word(page_summary: str, relevant_words: List[str]) -> bool:
    non_punctuated_summary = re.sub(r'[^\w\s]', '', page_summary)
    tokenized_summary = non_punctuated_summary.split(' ')
    return any(word in tokenized_summary for word in relevant_words)

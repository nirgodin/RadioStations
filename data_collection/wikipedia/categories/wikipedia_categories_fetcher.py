from typing import Dict, List

import pandas as pd
from tqdm import tqdm

from consts.data_consts import ARTIST_NAME
from consts.miscellaneous_consts import UTF_8_ENCODING
from consts.path_consts import WIKIPEDIA_CATEGORIES_PATH, WIKIPEDIA_CATEGORIES_MEMBERS_PATH
from consts.wikipedia_consts import CATEGORY
from data_collection.wikipedia.wikipedia_manager import WikipediaManager
from utils import read_json


class WikipediaCategoriesFetcher:
    def __init__(self, wikipedia_manager: WikipediaManager = WikipediaManager()):
        self._wikipedia_manager = wikipedia_manager

    def fetch(self):
        categories = read_json(WIKIPEDIA_CATEGORIES_PATH)
        records = []

        with tqdm(total=len(categories)) as progress_bar:
            for category in categories:
                members_records = self._fetch_single_category_members(category)
                records.extend(members_records)
                progress_bar.update(1)

        data = pd.DataFrame.from_records(records)
        data.to_csv(WIKIPEDIA_CATEGORIES_MEMBERS_PATH, index=False, encoding=UTF_8_ENCODING)

    def _fetch_single_category_members(self, category: str) -> List[Dict[str, str]]:
        category_page = self._wikipedia_manager.get_hebrew_page_directly(category)
        members_records = []

        for member_name, member_page in category_page.categorymembers.items():
            record = {
                ARTIST_NAME: member_name,
                CATEGORY: category
            }
            members_records.append(record)

        return members_records


if __name__ == '__main__':
    WikipediaCategoriesFetcher().fetch()

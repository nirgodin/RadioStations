import asyncio
from typing import List

from wikipediaapi import Wikipedia

from consts.data_consts import ARTIST_NAME, ARTIST_POPULARITY
from consts.path_consts import WIKIPEDIA_AGE_OUTPUT_PATH
from data_collection.wikipedia.age.base_wikipedia_age_collector import BaseWikipediaAgeCollector
from utils.data_utils import read_merged_data, extract_column_existing_values
from utils.regex_utils import contains_any_hebrew_character


class WikipediaAgeNameCollector(BaseWikipediaAgeCollector):
    def __init__(self):
        super().__init__()

    def _get_contender_artists(self) -> List[str]:
        data = read_merged_data()
        data.dropna(subset=[ARTIST_NAME], inplace=True)
        data.drop_duplicates(subset=[ARTIST_NAME], inplace=True)
        data.sort_values(by=[ARTIST_POPULARITY], ascending=False, inplace=True)

        return data[ARTIST_NAME].tolist()

    def _get_existing_artists(self) -> List[str]:
        return extract_column_existing_values(WIKIPEDIA_AGE_OUTPUT_PATH, ARTIST_NAME)

    def _get_artist_wikipedia_name(self, artist_detail: str) -> str:
        return artist_detail

    def _get_artist_spotify_name(self, artist_detail: str) -> str:
        return artist_detail

    def _get_wikipedia_abbreviation(self, artist_detail: str) -> str:
        return "he" if contains_any_hebrew_character(artist_detail) else "en"


if __name__ == '__main__':
    collector = WikipediaAgeNameCollector()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(collector.collect())

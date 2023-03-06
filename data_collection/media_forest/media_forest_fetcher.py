from typing import List, Dict

from pandas import DataFrame
from tqdm import tqdm

from consts.data_consts import ARTIST_NAME, NAME
from consts.media_forest_consts import MEDIA_FOREST_MENUS_MAPPING, MEDIA_FOREST_URL, \
    MEDIA_FOREST_WEEKLY_CHARTS_XPATH_FORMAT, PHOTOS_COLUMN_NAME, ARTIST_SONG_COLUMN_NAME, ARTISTS_COLUMNS_SPLIT, RANK, \
    RANK_ORIGINAL_COLUMN_NAME, SONGS_COLUMNS_SPLIT, ARTISTS_RENAME_MAPPING, SONGS_RENAME_MAPPING
from consts.miscellaneous_consts import UTF_8_ENCODING
from consts.path_consts import MEDIAFOREST_PATH_FORMAT
from tools.website_tables_crawler import WebsiteTablesCrawler
from utils.general_utils import get_current_datetime


class MediaForestFetcher:
    def __init__(self):
        self._tables_crawler = WebsiteTablesCrawler(
            url=MEDIA_FOREST_URL,
            xpath_format=MEDIA_FOREST_WEEKLY_CHARTS_XPATH_FORMAT
        )

    def fetch(self):
        raw_results = self._fetch_raw_results()

        with tqdm(total=len(raw_results)) as progress_bar:
            for name, data in raw_results.items():
                pre_processed_data = self._pre_process_single_result_data(name, data)
                now = get_current_datetime()
                path = MEDIAFOREST_PATH_FORMAT.format(name, now)
                pre_processed_data.to_csv(path, index=False, encoding=UTF_8_ENCODING)
                progress_bar.update(1)

    def _fetch_raw_results(self) -> Dict[str, List[DataFrame]]:
        with self._tables_crawler as tables_crawler:
            xpath_indexes = list(MEDIA_FOREST_MENUS_MAPPING.keys())
            results = tables_crawler.crawl(xpath_indexes)

        return {MEDIA_FOREST_MENUS_MAPPING[menu_number]: result for menu_number, result in results.items()}

    def _pre_process_single_result_data(self, name: str, data: List[DataFrame]) -> DataFrame:
        df = data[0]

        if name.__contains__('songs'):
            return self._pre_process_data(df, SONGS_COLUMNS_SPLIT, SONGS_RENAME_MAPPING)
        else:
            return self._pre_process_data(df, ARTISTS_COLUMNS_SPLIT, ARTISTS_RENAME_MAPPING)

    @staticmethod
    def _pre_process_data(original_data: DataFrame,
                          split_columns_mapping: Dict[str, List[str]],
                          rename_mapping: Dict[str, str]) -> DataFrame:
        data = original_data.copy(deep=True)
        data.drop(PHOTOS_COLUMN_NAME, axis=1, inplace=True)

        for original_column_name, target_column_names in split_columns_mapping.items():
            data[target_column_names] = data[original_column_name].str.split('  ', expand=True)
            data.drop(original_column_name, axis=1, inplace=True)

        data.rename(columns=rename_mapping, inplace=True)

        return data


if __name__ == '__main__':
    MediaForestFetcher().fetch()

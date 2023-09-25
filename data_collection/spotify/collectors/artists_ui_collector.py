import asyncio
from functools import partial
from typing import List, Dict

import pandas as pd
from asyncio_pool import AioPool
from bs4 import BeautifulSoup
from selenium.webdriver import Chrome
from selenium.webdriver.common.by import By
from tqdm import tqdm

from consts.data_consts import SCRAPED_AT, ARTIST_ID
from consts.path_consts import ARTISTS_UI_DETAILS_OUTPUT_PATH
from consts.spotify_ui_consts import ARTIST_ABOUT_CLICK_BUTTON_CSS_SELECTOR, ARTIST_PAGE_URL_FORMAT
from tools.data_chunks_generator import DataChunksGenerator
from tools.website_crawling.html_element import HTMLElement
from tools.website_crawling.web_element import WebElement
from tools.website_crawling.web_elements_extractor import WebElementsExtractor
from utils.data_utils import read_merged_data, extract_column_existing_values
from utils.datetime_utils import get_current_datetime
from utils.dict_utils import merge_dicts
from utils.file_utils import append_to_csv
from utils.selenium_utils import open_window, driver_session, switch_window


class SpotifyArtistUICollector:
    def __init__(self, chunk_size: int, max_chunks_number: int, sleep_time: int = 5):
        self._sleep_time = sleep_time
        self._max_chunks_number = max_chunks_number
        self._date = get_current_datetime()
        self._web_elements_extractor = WebElementsExtractor()
        self._data_chunks_generator = DataChunksGenerator(chunk_size, max_chunks_number)

    async def collect(self):
        await self._data_chunks_generator.execute_by_chunk(
            lst=self._get_unique_artists_ids(),
            filtering_list=extract_column_existing_values(ARTISTS_UI_DETAILS_OUTPUT_PATH, ARTIST_ID),
            func=self._collect_single_chunk
        )

    @staticmethod
    def _get_unique_artists_ids() -> List[str]:
        data = read_merged_data()
        return data[ARTIST_ID].dropna().unique().tolist()

    async def _collect_single_chunk(self, chunk: List[str]) -> None:
        records = await self._collect_raw_records(chunk)
        valid_records = [record for record in records if isinstance(record, dict)]

        if valid_records:
            data = pd.DataFrame.from_records(valid_records)
            data[SCRAPED_AT] = self._date
            append_to_csv(data, ARTISTS_UI_DETAILS_OUTPUT_PATH)

    async def _collect_raw_records(self, artist_ids: List[str]) -> List[dict]:
        pool = AioPool(10)

        with driver_session() as driver:
            with tqdm(total=len(artist_ids)) as progress_bar:
                func = partial(self._collect_single_artist_wrapper, driver, progress_bar)
                records = await pool.map(func, artist_ids)

        return records

    async def _collect_single_artist_wrapper(self, driver: Chrome, progress_bar: tqdm, artist_id: str) -> Dict[str, str]:
        try:
            return await self._collect_single_artist(driver, artist_id)

        except Exception as e:
            print(f"Failed to collect data for artist id `{artist_id}` due to the following exception:\n{e}")

        finally:
            progress_bar.update(1)

    async def _collect_single_artist(self, driver: Chrome, artist_id: str) -> dict:
        await self._load_artist_page(driver, artist_id)
        details = await self._collect_artist_details(driver, artist_id)
        details[ARTIST_ID] = artist_id

        return details

    async def _load_artist_page(self, driver: Chrome, artist_id: str) -> None:
        open_window(driver, artist_id)
        artist_url = ARTIST_PAGE_URL_FORMAT.format(artist_id)
        switch_window(driver, artist_id)
        driver.get(artist_url)
        await asyncio.sleep(self._sleep_time)
        switch_window(driver, artist_id)
        about_section_button = driver.find_element(by=By.CSS_SELECTOR, value=ARTIST_ABOUT_CLICK_BUTTON_CSS_SELECTOR)
        about_section_button.click()

    async def _collect_artist_details(self, driver: Chrome, artist_id: str) -> Dict[str, str]:
        switch_window(driver, artist_id)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        artist_details = []

        for element in self._web_elements:
            detail = self._web_elements_extractor.extract(soup, element)
            artist_details.extend(detail)

        return merge_dicts(*artist_details)

    @property
    def _web_elements(self) -> List[WebElement]:
        return [
            WebElement(
                name='about',
                type=HTMLElement.DIV,
                class_='Type__TypeElement-sc-goli3j-0 fZDcWX CjnwbSTpODW56Gerg7X6'
            ),
            WebElement(
                name='social_links',
                type=HTMLElement.A,
                class_='oe0FHRJU7PvjoTnXJmfr',
                multiple=True
            ),
            WebElement(
                name='monthly_listeners',
                type=HTMLElement.DIV,
                class_='Type__TypeElement-sc-goli3j-0 fAJsTt'
            ),
            WebElement(
                name='world_number',
                type=HTMLElement.DIV,
                class_='Type__TypeElement-sc-goli3j-0 bnCeva'
            ),
            WebElement(
                name='top_city',
                type=HTMLElement.DIV,
                class_='Q_OUHp7iDNLBcO2ZYI2x',
                multiple=True,
                enumerate=True
            )
        ]

from time import sleep
from typing import List, Dict, Optional

import pandas as pd
from pandas import DataFrame
from selenium import webdriver
from selenium.webdriver.common.by import By


class WebsiteTablesCrawler:
    def __init__(self, url: str, xpath_format: str, sleep_between: float = 0.5):
        self._url = url
        self._xpath_format = xpath_format
        self._driver: Optional[webdriver.Chrome] = None
        self._sleep_between = sleep_between

    def crawl(self, xpath_indexes: List[str]) -> Dict[str, List[DataFrame]]:
        self._driver.get(self._url)
        results = {}

        for i in xpath_indexes:
            result = self._extract_single_route_tables(i)
            results[i] = result

        return results

    def _extract_single_route_tables(self, i: str) -> List[DataFrame]:
        xpath = self._xpath_format.format(i)
        element = self._driver.find_element(by=By.XPATH, value=xpath)
        element.click()
        sleep(self._sleep_between)

        return pd.read_html(self._driver.page_source)

    def __enter__(self) -> 'WebsiteCrawler':
        self._driver = webdriver.Chrome()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._driver is not None:
            self._driver.close()

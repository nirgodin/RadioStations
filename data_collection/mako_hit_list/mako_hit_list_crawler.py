from typing import List

import pandas as pd
import requests
from bs4 import BeautifulSoup
from pandas import DataFrame

from consts.mako_hit_list_consts import MAKO_HIT_LIST_ROUTES, MAKO_HIT_LIST_URL_FORMAT, MAKO_HIT_LIST_WEB_ELEMENTS
from consts.path_consts import MAKO_HIT_LIST_OUTPUT_PATH_FORMAT
from tools.website_crawling.web_elements_extractor import WebElementsExtractor
from utils.datetime_utils import get_current_datetime
from utils.file_utils import to_csv


class MakoHitListCrawler:
    def __init__(self):
        self._web_elements_extractor = WebElementsExtractor()

    def crawl(self):
        charts_data: List[DataFrame] = []

        for route_name, route_endpoint in MAKO_HIT_LIST_ROUTES.items():
            chart_data = self._crawl_single_route(route_name, route_endpoint)
            charts_data.append(chart_data)

        data = pd.concat(charts_data).reset_index(drop=True)
        self._to_csv(data)

    def _crawl_single_route(self, route_name: str, route_endpoint: str) -> DataFrame:
        route_url = MAKO_HIT_LIST_URL_FORMAT.format(route_endpoint)
        page_content = requests.get(route_url).content
        soup = BeautifulSoup(page_content, "html.parser")
        route_data = self._extract_single_route_data(soup)
        route_data['chart_name'] = route_name

        return route_data

    def _extract_single_route_data(self, soup: BeautifulSoup) -> DataFrame:
        data = {}

        for element in MAKO_HIT_LIST_WEB_ELEMENTS:
            results = self._web_elements_extractor.extract(soup, element)
            data[element.name] = [result[element.name] for result in results]

        return pd.DataFrame(data)

    @staticmethod
    def _to_csv(data: DataFrame) -> None:
        now = get_current_datetime()
        output_path = MAKO_HIT_LIST_OUTPUT_PATH_FORMAT.format(now)

        to_csv(data=data, output_path=output_path)


if __name__ == '__main__':
    MakoHitListCrawler().crawl()

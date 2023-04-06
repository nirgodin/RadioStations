from typing import List

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag
from pandas import DataFrame

from consts.html_consts import CLASS
from consts.mako_hit_list_consts import MAKO_HIT_LIST_ROUTES, MAKO_HIT_LIST_URL_FORMAT, MAKO_HIT_LIST_WEB_ELEMENTS
from consts.path_consts import MAKO_HIT_LIST_OUTPUT_PATH_FORMAT
from data_collection.mako_hit_list.web_element import WebElement
from utils.file_utils import to_csv
from utils.datetime_utils import get_current_datetime


class MakoHitListCrawler:
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
            results = self._recursively_extract_web_elements_results(soup, element)
            data[element.name] = [result.text for result in results]

        return pd.DataFrame(data)

    def _recursively_extract_web_elements_results(self, soup: BeautifulSoup, web_element: WebElement):
        results = soup.find_all(web_element.element_type, {CLASS: web_element.class_name})

        if web_element.child_element is None:
            return results
        else:
            return self._extract_child_elements_results(results, web_element.child_element)

    def _extract_child_elements_results(self, father_element_results: List[Tag], child_element: WebElement) -> List[
        Tag]:
        child_results = []

        for result in father_element_results:
            child_result = result.find_next(child_element.element_type, {CLASS, child_element.class_name})
            child_results.append(child_result)

        if child_element.child_element is None:
            return child_results
        else:
            return self._extract_child_elements_results(
                father_element_results=child_results,
                child_element=child_element.child_element
            )

    @staticmethod
    def _to_csv(data: DataFrame) -> None:
        now = get_current_datetime()
        output_path = MAKO_HIT_LIST_OUTPUT_PATH_FORMAT.format(now)

        to_csv(data=data, output_path=output_path)


if __name__ == '__main__':
    MakoHitListCrawler().crawl()

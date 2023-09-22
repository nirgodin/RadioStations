from typing import List, Optional, Dict

from bs4 import BeautifulSoup, Tag

from tools.website_crawling.html_element import HTMLElement
from tools.website_crawling.web_element import WebElement


class WebElementsExtractor:
    def extract(self, soup: BeautifulSoup, web_element: WebElement) -> List[Tag]:
        if web_element.multiple:
            return self._extract_multiple_details(soup, web_element)

    def _extract_multiple_details(self, soup: BeautifulSoup, web_element: WebElement) -> List[Tag]:
        tags = soup.find_all(web_element.type.value, class_=web_element.class_)
        if web_element.child_element is not None:
            tags = [self._extract_child_elements_tags(tag, web_element.child_element) for tag in tags]

        if web_element.enumerate:
            return [self._extract_single_detail(tag, web_element, i + 1) for i, tag in enumerate(tags)]
        else:
            return [self._extract_single_detail(tag, web_element) for tag in tags]

    def _extract_child_elements_tags(self, father_element_tag: Tag, child_element: WebElement) -> List[Tag]:
        child_tag = father_element_tag.find_next(child_element.type.value, class_=child_element.class_)

        if child_element.child_element is None:
            return child_tag
        else:
            return self._extract_child_elements_tags(
                father_element_tag=child_tag,
                child_element=child_element.child_element
            )

    @staticmethod
    def _extract_single_detail(tag: Tag, web_element: WebElement, number: Optional[int] = None) -> Optional[Dict[str, str]]:
        if tag is None:
            return
        elif web_element.type == HTMLElement.A:
            return {tag.text: tag["href"]}
        elif number is None:
            return {web_element.name: tag.text}
        else:
            return {f"{web_element.name}{number}": tag.text}

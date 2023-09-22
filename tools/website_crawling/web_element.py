from dataclasses import dataclass
from typing import Optional

from tools.website_crawling.html_element import HTMLElement


@dataclass
class WebElement:
    name: str
    type: HTMLElement
    class_: str
    child_element: Optional['WebElement'] = None
    multiple: bool = False
    enumerate: bool = True

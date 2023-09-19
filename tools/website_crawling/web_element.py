from dataclasses import dataclass
from typing import Optional


@dataclass
class WebElement:
    name: str
    element_type: str
    class_name: str
    child_element: Optional['WebElement'] = None

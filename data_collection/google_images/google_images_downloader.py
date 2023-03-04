import os
import re
from typing import List

from icrawler.builtin import GoogleImageCrawler

from consts.path_consts import GOOGLE_IMAGES_RESOURCES_PATH


class GoogleImagesDownloader:
    def __init__(self, images_per_artist: int):
        self._images_per_artist = images_per_artist
        self._invalid_os_chars_regex = re.compile(r'[/:\"]')
        self._images_paths = None

    def download(self, artist: str) -> List[str]:
        self._crawl(artist)
        self._images_paths = self._get_image_paths()

        return self._images_paths

    def _crawl(self, artist: str) -> None:
        crawler = GoogleImageCrawler(storage={'root_dir': GOOGLE_IMAGES_RESOURCES_PATH})
        crawler.crawl(keyword=f'{artist} musician', max_num=self._images_per_artist)

    @staticmethod
    def _get_image_paths() -> List[str]:
        images_paths = []

        for file in os.listdir(GOOGLE_IMAGES_RESOURCES_PATH):
            file_path = os.path.join(GOOGLE_IMAGES_RESOURCES_PATH, file)

            if os.path.isfile(file_path):
                images_paths.append(file_path)

        return images_paths

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for image_path in self._images_paths:
            os.remove(image_path)

        self._images_paths = None

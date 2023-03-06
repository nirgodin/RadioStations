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
        self._remove_images(images_paths=self._get_image_paths())

    def download(self, artist: str) -> List[str]:
        try:
            return self._download(artist)

        except:
            print('Received exception! returning empty list')
            return []

    def _download(self, artist: str) -> List[str]:
        crawler = GoogleImageCrawler(storage={'root_dir': GOOGLE_IMAGES_RESOURCES_PATH})
        crawler.crawl(keyword=f'{artist} musician', max_num=self._images_per_artist)
        self._images_paths = self._get_image_paths()

        return self._images_paths

    @staticmethod
    def _get_image_paths() -> List[str]:
        images_paths = []

        for file in os.listdir(GOOGLE_IMAGES_RESOURCES_PATH):
            file_path = os.path.join(GOOGLE_IMAGES_RESOURCES_PATH, file)

            if os.path.isfile(file_path):
                images_paths.append(file_path)

        return images_paths

    def _remove_images(self, images_paths: List[str], retries_left: int = 3) -> None:
        if retries_left == 0:
            raise PermissionError()

        for image_path in images_paths:
            try:
                os.remove(image_path)
                images_paths.remove(image_path)

            except PermissionError:
                return self._remove_images(images_paths, retries_left=retries_left-1)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._images_paths is None:
            return

        self._remove_images(self._images_paths)
        self._images_paths = None

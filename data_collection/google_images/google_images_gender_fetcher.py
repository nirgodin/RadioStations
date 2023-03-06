import os.path
from collections import Counter
from random import randint
from statistics import mean
from time import sleep
from typing import List, Union, Dict

import numpy as np
import pandas as pd
from pandas import DataFrame
from tqdm import tqdm

from consts.data_consts import ARTIST_NAME, NAME, CONFIDENCE
from consts.openai_consts import ARTIST_GENDER
from consts.path_consts import MERGED_DATA_PATH, GOOGLE_IMAGES_GENDER_PATH
from data_collection.google_images.google_images_downloader import GoogleImagesDownloader
from data_collection.wikipedia.gender.genders import Genders
from tools.data_chunks_generator import DataChunksGenerator
from tools.image_detection.gender_image_detector import GenderImageDetector
from utils.file_utils import append_to_csv

PLAY_COUNT = 'play_count'


class GoogleImagesGenderFetcher:
    def __init__(self, chunk_size: int = 50, images_per_artist: int = 5, confidence_threshold: float = 0.5):
        self._data_chunks_generator = DataChunksGenerator(chunk_size)
        self._images_per_artist = images_per_artist
        self._gender_image_detector = GenderImageDetector(confidence_threshold)
        self._image_downloader = GoogleImagesDownloader(images_per_artist)

    def fetch(self) -> None:
        chunks = self._data_chunks_generator.generate_data_chunks(
            lst=self._get_relevant_artists(),
            filtering_list=self._get_existing_artists()
        )

        for chunk in chunks:
            records = self._fetch_single_chunk_records(chunk)
            chunk_data = pd.DataFrame.from_records(records)
            append_to_csv(data=chunk_data, output_path=GOOGLE_IMAGES_GENDER_PATH)
            sleep(randint(10, 50))

    def _fetch_single_chunk_records(self, chunk: List[str]) -> List[Dict[str, Union[str, float]]]:
        records = []

        with tqdm(total=len(chunk)) as progress_bar:
            for artist in chunk:
                with self._image_downloader as image_downloader:
                    images_paths = image_downloader.download(artist)
                    record = self._get_images_predictions(artist, images_paths)

                records.append(record)
                progress_bar.update(1)

        return records

    def _get_images_predictions(self, artist: str, images_paths: List[str]) -> Dict[str, Union[str, float]]:
        predictions = [self._get_single_image_prediction(path) for path in images_paths]

        if not predictions:
            return {ARTIST_NAME: artist}

        predictions_count = Counter([prediction[ARTIST_GENDER] for prediction in predictions])
        most_common_prediction, most_common_prediction_count = predictions_count.most_common(1)[0]
        confidence = mean([prediction[CONFIDENCE] for prediction in predictions
                           if prediction[ARTIST_GENDER] == most_common_prediction])

        return {
            ARTIST_NAME: artist,
            ARTIST_GENDER: most_common_prediction,
            CONFIDENCE: confidence,
            'prediction_share': most_common_prediction_count / self._images_per_artist,
            'images_number': self._images_per_artist
        }

    def _get_single_image_prediction(self, image_path: str) -> Dict[str, Union[str, float]]:
        predictions = self._gender_image_detector.detect_gender(image_path)

        if not predictions:
            artist_gender = ''
            confidence = np.nan

        elif len(predictions) == 1:
            artist_gender, confidence = predictions[0].values()

        else:
            artist_gender = Genders.BAND.value
            confidence = mean([prediction[CONFIDENCE] for prediction in predictions])

        return {
            ARTIST_GENDER: artist_gender,
            CONFIDENCE: confidence
        }

    def _get_relevant_artists(self):
        play_count = self._get_artists_play_count()
        genders = pd.read_csv(r'data/mapped_genders.csv')
        merged = genders.merge(right=play_count, how='left', on=ARTIST_NAME)
        merged.sort_values(by=PLAY_COUNT, ascending=False, inplace=True)
        no_label_artists = merged[merged[ARTIST_GENDER].isna()].reset_index(drop=True)

        return no_label_artists[ARTIST_NAME]

    @staticmethod
    def _get_artists_play_count() -> DataFrame:
        data = pd.read_csv(MERGED_DATA_PATH)
        play_count = data.groupby(ARTIST_NAME).count()
        play_count.reset_index(level=0, inplace=True)
        play_count = play_count[[ARTIST_NAME, NAME]]
        play_count.columns = [ARTIST_NAME, PLAY_COUNT]

        return play_count

    @staticmethod
    def _get_existing_artists() -> List[str]:
        if not os.path.exists(GOOGLE_IMAGES_GENDER_PATH):
            return []

        data = pd.read_csv(GOOGLE_IMAGES_GENDER_PATH)
        return data[ARTIST_NAME].tolist()


if __name__ == '__main__':
    GoogleImagesGenderFetcher(chunk_size=20).fetch()

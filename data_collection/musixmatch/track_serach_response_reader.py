from statistics import mean
from typing import List, Dict, Union

from pandas import Series

from consts.data_consts import TRACK, ARTIST_NAME, NAME, POPULARITY
from consts.musixmatch_consts import MESSAGE, BODY, TRACK_LIST, HEADER, STATUS_CODE, OK_STATUS_CODE, TRACK_NAME, \
    TRACK_RATING
from utils.general_utils import get_similarity_score


class TrackSearchResponseReader:
    def read(self, response: dict, row: Series) -> dict:
        track_list = self._get_response_track_list(response)

        if not track_list:
            return {}

        most_similar_track = self._extract_most_similar_track(track_list, row)
        return most_similar_track[TRACK]

    @staticmethod
    def _get_response_track_list(response: dict) -> list:
        status_code = response.get(MESSAGE, {}).get(HEADER, {}).get(STATUS_CODE)

        if status_code != OK_STATUS_CODE:
            return []

        return response.get(MESSAGE, {}).get(BODY, {}).get(TRACK_LIST, [])

    def _extract_most_similar_track(self, track_list: List[dict], row: Series) -> dict:
        if len(track_list) == 1:
            return track_list[0]

        tracks_similarity_scores = self._get_tracks_similarity_scores(track_list, row)
        most_similar_track_index = max(tracks_similarity_scores, key=tracks_similarity_scores.get)

        return track_list[most_similar_track_index]

    def _get_tracks_similarity_scores(self, track_list: List[dict], row: Series) -> Dict[int, float]:
        original_track_name = row[NAME]
        original_artist_name = row[ARTIST_NAME]
        original_popularity = row[POPULARITY]
        similarity_scores = {}

        for i, track in enumerate(track_list):
            score = self._create_single_track_similarity_score(
                original_track_name=original_track_name,
                original_artist_name=original_artist_name,
                original_popularity=original_popularity,
                track=track
            )
            similarity_scores[i] = score

        return similarity_scores

    def _create_single_track_similarity_score(self,
                                              original_track_name: str,
                                              original_artist_name: str,
                                              original_popularity: int,
                                              track: dict) -> float:
        current_track_name = self._extract_single_track_field(track, TRACK_NAME)
        current_artist_name = self._extract_single_track_field(track, ARTIST_NAME)
        current_popularity = self._extract_single_track_field(track, TRACK_RATING)
        raw_similarities = [
            get_similarity_score(original_track_name, current_track_name),
            get_similarity_score(original_artist_name, current_artist_name),
            self._get_popularity_similarity_score(original_popularity, current_popularity)
        ]

        return mean(raw_similarities)

    @staticmethod
    def _extract_single_track_field(track: dict, field_name: str) -> Union[str, int]:
        return track.get(TRACK, {}).get(field_name, '')

    @staticmethod
    def _get_popularity_similarity_score(original_popularity: int, current_popularity: int) -> float:
        popularity_diff = abs(original_popularity - current_popularity) / 100
        return 1 - popularity_diff

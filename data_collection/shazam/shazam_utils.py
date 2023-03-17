from typing import List

from pandas import DataFrame

from consts.miscellaneous_consts import UTF_8_ENCODING
from consts.shazam_consts import TRACKS, LOCATION
from data_collection.shazam.shazam_track_response_extractor import ShazamTrackResponseExtractor
from utils.datetime_utils import get_current_datetime


def extract_tracks_data(tracks_data: dict, location: str) -> DataFrame:
    tracks: List[dict] = tracks_data[TRACKS]
    track_response_extractor = ShazamTrackResponseExtractor()
    tracks_data = track_response_extractor.extract_multiple_tracks_info(tracks)
    tracks_data[LOCATION] = location

    return tracks_data


def to_csv(data: DataFrame, output_path_format: str) -> None:
    now = get_current_datetime()
    output_path = output_path_format.format(now)

    data.to_csv(output_path, index=False, encoding=UTF_8_ENCODING)

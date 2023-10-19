from typing import List

import pandas as pd
from pandas import DataFrame

from consts.data_consts import ID, ARTISTS
from consts.shazam_consts import SHAZAM_TRACK_KEY, TITLE, SUBTITLE, HUB, ACTIONS, APPLE_MUSIC_TRACK_ID, EXPLICIT, \
    APPLE_MUSIC_ADAM_ID


class ShazamTrackResponseExtractor:
    def extract_multiple_tracks_info(self, tracks: List[dict]) -> DataFrame:
        tracks_info = []

        for track in tracks:
            track_info = self.extract_single_track_info(track)
            tracks_info.append(track_info)

        return pd.DataFrame.from_records(tracks_info)

    def extract_single_track_info(self, track: dict) -> dict:
        return {
            SHAZAM_TRACK_KEY: track[SHAZAM_TRACK_KEY],
            TITLE: track[TITLE],
            SUBTITLE: track[SUBTITLE],
            APPLE_MUSIC_TRACK_ID: self._extract_apple_music_track_id(track),
            EXPLICIT: self._is_explicit_track(track),
            APPLE_MUSIC_ADAM_ID: self._extract_track_adam_id(track),
        }

    @staticmethod
    def _extract_apple_music_track_id(track: dict) -> str:
        track_actions: List[dict] = track.get(HUB, {}).get(ACTIONS, [])

        if not track_actions:
            return ''

        for action in track_actions:
            if ID in action.keys():
                return action.get(ID, '')

        return ''

    @staticmethod
    def _is_explicit_track(track: dict) -> bool:
        return track.get(HUB, {}).get(EXPLICIT, False)

    @staticmethod
    def _extract_track_adam_id(track: dict) -> str:
        track_artists = track.get(ARTISTS, [])

        if not track_artists:
            return ''

        first_artist = track_artists[0]

        return first_artist.get(APPLE_MUSIC_ADAM_ID, '')

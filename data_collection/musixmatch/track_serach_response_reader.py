from typing import Optional

from consts.data_consts import TRACK
from consts.musixmatch_consts import MESSAGE, BODY, TRACK_LIST, HEADER, STATUS_CODE, OK_STATUS_CODE


class TrackSearchResponseReader:
    def read(self, response: dict) -> Optional[dict]:
        track_list = self._get_response_track_list(response)

        if not track_list:
            return

        first_track = track_list[0]
        return first_track[TRACK]

    @staticmethod
    def _get_response_track_list(response: dict) -> list:
        status_code = response.get(MESSAGE, {}).get(HEADER, {}).get(STATUS_CODE)

        if status_code != OK_STATUS_CODE:
            return []

        return response.get(MESSAGE, {}).get(BODY, {}).get(TRACK_LIST, [])

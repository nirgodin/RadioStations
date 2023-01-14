from typing import Optional

from consts.data_consts import TRACK

MESSAGE = 'message'
BODY = 'body'
TRACK_LIST = 'track_list'


class TrackSearchResponseReader:
    def read(self, response: dict) -> Optional[dict]:
        track_list = self._get_response_track_list(response)

        if not track_list:
            return

        first_track = track_list[0]
        return first_track[TRACK]

    @staticmethod
    def _get_response_track_list(response: dict) -> list:
        return response.get(MESSAGE, {}).get(BODY, {}).get(TRACK_LIST, [])

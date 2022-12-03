import json
from typing import Any, Dict, Optional, Generator, List
import requests

CHARTS_LIST = 'charts/list'
CHARTS_TRACK = 'charts/track'

BASE_RAPIDAPI_URL = "https://shazam.p.rapidapi.com"
SHAZAM_CREDS_PATH = '../shazam_creds.json'


class Shazammer:
    def __init__(self):
        self._base_url = BASE_RAPIDAPI_URL

    def get_multiple_charts_tracks(self, charts_ids: List[str],
                                   number_of_tracks: int) -> Dict[str, List[Dict[str, Any]]]:
        charts_tracks = {}

        for id in charts_ids:
            chart_tracks = list(self._get_single_chart_tracks(chart_list_id=id, number_of_tracks=number_of_tracks))
            charts_tracks.update({id: chart_tracks})

        return charts_tracks

    def _get_single_chart_tracks(self, chart_list_id: str,
                                 number_of_tracks: int) -> Generator[Dict[str, Any], None, None]:
        start_from_steps = list(range(0, number_of_tracks, 20))

        for step in start_from_steps:
            yield self._get_single_chart_tracks_from(chart_list_id=chart_list_id, start_from=step)

    def _get_single_chart_tracks_from(self, chart_list_id: str, start_from: int) -> Dict[str, Any]:
        query = self._create_query_string(chart_list_id, start_from=start_from)
        return self._request(route=CHARTS_TRACK, params=query)

    def get_charts_list(self) -> Dict[str, Any]:
        return self._request(route=CHARTS_LIST)

    def _request(self, route: str, params: Dict[str, str] = None) -> Dict[str, Any]:
        response = requests.request(
            method="GET",
            url=f'{self._base_url}/{route}',
            headers=self._headers,
            params=params
        )

        if response.status_code == 200:
            return response.json()
        else:
            raise ConnectionError(response.reason)

    @staticmethod
    def _create_query_string(list_id: str, locale: Optional[str] = "en-US",
                             page_size: Optional[int] = 20, start_from: Optional[int] = 0) -> Dict[str, str]:
        return {
            "locale": locale,
            "listId": list_id,
            "pageSize": str(page_size),
            "startFrom": str(start_from)
        }

    @property
    def _headers(self):
        with open(SHAZAM_CREDS_PATH) as f:
            return json.load(f)

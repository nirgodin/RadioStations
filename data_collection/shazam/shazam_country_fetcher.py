from shazamio import Shazam

from consts.path_consts import SHAZAM_ISRAEL_PATH_FORMAT, SHAZAM_WORLD_PATH_FORMAT
from consts.shazam_consts import ISRAEL_COUNTRY_CODE, ISRAEL, WORLD
from data_collection.shazam.shazam_utils import extract_tracks_data, to_csv


class ShazamCountryFetcher:
    def __init__(self, shazam: Shazam = Shazam()):
        self._shazam = shazam

    async def fetch_country_top_tracks(self, country_code: str = ISRAEL_COUNTRY_CODE) -> None:
        israel_top_tracks = await self._shazam.top_country_tracks(country_code=country_code, limit=200)
        tracks_data = extract_tracks_data(tracks_data=israel_top_tracks, location=ISRAEL)

        to_csv(data=tracks_data, output_path_format=SHAZAM_ISRAEL_PATH_FORMAT)

    async def fetch_world_top_tracks(self) -> None:
        world_top_tracks = await self._shazam.top_world_tracks(limit=200)
        tracks_data = extract_tracks_data(tracks_data=world_top_tracks, location=WORLD)

        to_csv(data=tracks_data, output_path_format=SHAZAM_WORLD_PATH_FORMAT)

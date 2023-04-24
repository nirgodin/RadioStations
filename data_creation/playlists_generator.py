import asyncio
import os
from typing import List

from data_creation.data_filterer import DataFilterer
from data_creation.playlists_creator import PlaylistsCreator
from data_creation.query_condition import QueryCondition
from consts.data_consts import URI


class PlaylistsGenerator:
    def __init__(self):
        self._data_filterer = DataFilterer()
        self._playlists_creator = PlaylistsCreator()

    async def generate(self, query_conditions: List[QueryCondition], user_id: str, public: bool = True) -> None:
        filtered_data = self._data_filterer.filter(query_conditions)
        uris = filtered_data[URI].tolist()
        await self._playlists_creator.create(uris=uris, user_id=user_id, public=public)


if __name__ == '__main__':
    query_conditions = [
        QueryCondition(
            column='duration_ms',
            operator='<',
            value=240000
        ),
        QueryCondition(
            column='energy',
            operator='<',
            value=0.8
        ),
        QueryCondition(
            column='energy',
            operator='<',
            value=0.8
        ),
        QueryCondition(
            column='energy',
            operator='>',
            value=0.4
        ),
        QueryCondition(
            column='explicit',
            operator='==',
            value=True
        ),
        QueryCondition(
            column='tempo',
            operator='>',
            value=100
        ),
        QueryCondition(
            column='median_popularity',
            operator='<',
            value=80
        ),
        QueryCondition(
            column='median_popularity',
            operator='>',
            value=40
        )
    ]
    user_id = os.environ['SPOTIFY_USER_ID']
    playlists_generator = PlaylistsGenerator()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(playlists_generator.generate(query_conditions, user_id))

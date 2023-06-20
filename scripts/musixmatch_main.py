import asyncio

import pandas as pd

from analysis.analyzers.lyrics_language.musixmatch_lyrics_language_analyzer import MusixmatchLyricsLanguageAnalyzer
from consts.path_consts import MERGED_DATA_PATH
from data_collection.musixmatch.musixmatch_lyrics_fetcher import MusixmatchLyricsFetcher
from data_collection.musixmatch.musixmatch_search_fetcher import MusixmatchSearchFetcher
from tools.environment_manager import EnvironmentManager


async def run(request_limit: int):
    EnvironmentManager().set_env_variables()

    search_fetcher = MusixmatchSearchFetcher(request_limit=request_limit)
    print(f'Starting to fetch {request_limit} tracks ids using MusixmatchSearchFetcher')
    await search_fetcher.fetch_tracks_ids(data=pd.read_csv(MERGED_DATA_PATH))

    lyrics_fetcher = MusixmatchLyricsFetcher(request_limit=request_limit - 300)
    print(f'Starting to fetch {request_limit} tracks lyrics using MusixmatchLyricsFetcher')
    await lyrics_fetcher.fetch_tracks_lyrics()

    language_analyzer = MusixmatchLyricsLanguageAnalyzer()
    print(f'Starting to analyze Musixmatch tracks lyrics language')
    language_analyzer.analyze()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(request_limit=1000))

from typing import Dict, List

from tqdm import tqdm

from analysis.analyzer_interface import IAnalyzer
from consts.musixmatch_consts import MUSIXMATCH_TRACK_ID, LYRICS_BODY
from consts.path_consts import MUSIXMATCH_TRACKS_LYRICS_PATH, MUSIXMATCH_FORMATTED_TRACKS_LYRICS_PATH
from utils.file_utils import read_json, to_json

MUSIXMATCH_STOP_LYRICS_TOKEN = "..."


class MusixmatchLyricsAnalyzer(IAnalyzer):
    def analyze(self) -> None:
        raw_lyrics = read_json(MUSIXMATCH_TRACKS_LYRICS_PATH)
        formatted_lyrics = self._format_tracks_lyrics(raw_lyrics)

        to_json(d=formatted_lyrics, path=MUSIXMATCH_FORMATTED_TRACKS_LYRICS_PATH)

    def _format_tracks_lyrics(self, raw_lyrics: Dict[str, dict]) -> Dict[str, List[str]]:
        formatted_lyrics = {}

        with tqdm(total=len(raw_lyrics)) as progress_bar:
            for spotify_track_id, musixmatch_lyrics_details in raw_lyrics.items():
                if musixmatch_lyrics_details:
                    formatted_track_lyrics = self._extract_single_formatted_lyrics(musixmatch_lyrics_details)
                    lyrics_id = musixmatch_lyrics_details[MUSIXMATCH_TRACK_ID]
                    formatted_lyrics[lyrics_id] = formatted_track_lyrics

                progress_bar.update(1)

        return formatted_lyrics

    @staticmethod
    def _extract_single_formatted_lyrics(musixmatch_lyrics_details: dict) -> List[str]:
        lyrics_body = musixmatch_lyrics_details[LYRICS_BODY]
        lyrics = []

        for line in lyrics_body.split("\n"):
            stripped_line = line.strip()

            if stripped_line == MUSIXMATCH_STOP_LYRICS_TOKEN:
                break

            if stripped_line:
                lyrics.append(stripped_line)

        return lyrics

    @property
    def name(self) -> str:
        return "Musixmatch lyrics analyzer"


if __name__ == '__main__':
    MusixmatchLyricsAnalyzer().analyze()

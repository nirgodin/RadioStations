import json

from sqlalchemy import Column, String, ForeignKey, ARRAY, SmallInteger, JSON, Enum, Float

from consts.lyrics_consts import LYRICS_SOURCE, NUMBER_OF_WORDS, WORDS_COUNT
from consts.musixmatch_consts import LYRICS
from database.orm_models.base_orm_model import BaseORMModel
from models.data_source import DataSource


class TrackLyrics(BaseORMModel):
    __tablename__ = "tracks_lyrics"

    id = Column(String, ForeignKey("spotify_tracks.id"), primary_key=True, nullable=False)
    lyrics = Column(ARRAY(String))
    lyrics_source = Column(Enum(DataSource))
    language = Column(String)
    language_confidence = Column(SmallInteger)
    number_of_words = Column(SmallInteger)
    words_count = Column(JSON)

    @staticmethod
    def _pre_process_record_items(record_items: dict) -> dict:
        lyrics_source = record_items[LYRICS_SOURCE]
        record_items[LYRICS_SOURCE] = None if lyrics_source is None else DataSource(lyrics_source)
        language_score = record_items["score"]
        record_items["language_confidence"] = None if language_score is None else round(language_score * 100)
        number_of_words = record_items[NUMBER_OF_WORDS]
        record_items[NUMBER_OF_WORDS] = None if number_of_words is None else int(number_of_words)
        record_items[LYRICS] = None if record_items[LYRICS] is None else eval(record_items[LYRICS])
        record_items[WORDS_COUNT] = None if record_items[WORDS_COUNT] is None else json.loads(record_items[WORDS_COUNT])

        return record_items

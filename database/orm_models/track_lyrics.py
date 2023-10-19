from sqlalchemy import Column, String, ForeignKey, ARRAY, SmallInteger, JSON

from database.orm_models.base_orm_model import BaseORMModel


class TrackLyrics(BaseORMModel):
    __tablename__ = "tracks_lyrics"

    id = Column(String, ForeignKey("spotify_tracks.id"), primary_key=True, nullable=False)
    lyrics = Column(ARRAY(String))
    language = Column(String)
    number_of_words = Column(SmallInteger)
    words_count = Column(JSON)

    @staticmethod
    def _pre_process_record_items(record_items: dict) -> dict:
        pass

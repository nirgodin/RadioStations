from typing import Union

from sqlalchemy import Column, String, ForeignKey

from consts.data_consts import GENIUS_ID
from consts.musixmatch_consts import MUSIXMATCH_ID
from consts.shazam_consts import APPLE_MUSIC_ADAM_ID, APPLE_MUSIC_ID
from data_processing.pre_processors.language.language_pre_processor import SHAZAM_KEY
from database.orm_models.base_orm_model import BaseORMModel
from utils.general_utils import stringify_float


class TrackIDMapping(BaseORMModel):
    __tablename__ = "track_ids_mapping"

    id = Column(String, ForeignKey("spotify_tracks.id"), primary_key=True, nullable=False)
    adam_id = Column(String)
    apple_music_id = Column(String)
    genius_id = Column(String)
    musixmatch_id = Column(String)
    shazam_id = Column(String)

    @staticmethod
    def _pre_process_record_items(record_items: dict) -> dict:
        record_items["adam_id"] = TrackIDMapping._pre_process_id(record_items[APPLE_MUSIC_ADAM_ID])
        record_items[GENIUS_ID] = TrackIDMapping._pre_process_id(record_items[GENIUS_ID])
        record_items[MUSIXMATCH_ID] = TrackIDMapping._pre_process_id(record_items[MUSIXMATCH_ID])
        record_items["shazam_id"] = TrackIDMapping._pre_process_id(record_items[SHAZAM_KEY])
        record_items[APPLE_MUSIC_ID] = TrackIDMapping._pre_process_id(record_items[APPLE_MUSIC_ID])

        return record_items

    @staticmethod
    def _pre_process_id(track_id: Union[None, float, str]) -> Union[None, str]:
        if track_id is None:
            return None

        if isinstance(track_id, float) or isinstance(track_id, int):
            return stringify_float(track_id)

        if track_id.endswith('.0'):
            return track_id.rstrip('.0')

        return track_id

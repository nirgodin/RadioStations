import asyncio

from component_factory import ComponentFactory
from database.orm_models.base_orm_model import Base
from database.orm_models.spotify_album import SpotifyAlbum
from database.orm_models.spotify_artist import SpotifyArtist
from database.orm_models.spotify_track import SpotifyTrack
from database.orm_models.audio_features import AudioFeatures
from database.orm_models.radio_track import RadioTrack
# TODO: Import dynamically all BaseORMModel sub classes to make sure they are created

from tools.environment_manager import EnvironmentManager


async def main():
    EnvironmentManager().set_env_variables()
    engine = ComponentFactory.get_database_engine()

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
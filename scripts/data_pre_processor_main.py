import asyncio
from datetime import datetime

from data_collectors.components import ComponentFactory
from spotipyio import SpotifyClient
from spotipyio.logic.authentication.spotify_session import SpotifySession

from consts.path_consts import MERGED_DATA_PATH
from data_processing.data_pre_processor import DataPreProcessor
from database.migration_script import DatabaseMigrator
from database.shazam_top_tracks_migration_script import ShazamTopTracksMigrationScript
from database.shazam_tracks_migration_script import ShazamTracksMigrationScript
from database.spotify_top_tracks_migration_script import SpotifyTopTracksMigrator
from research.playlists_creator_database.playlists_creator_database_generator import PlaylistsCreatorDatabaseGenerator
from tools.environment_manager import EnvironmentManager


async def run_spotify_top_tracks_migration() -> None:
    async with SpotifySession() as session:
        spotify_client = SpotifyClient.create(session)
        factory = ComponentFactory()
        insertions_manager = factory.spotify.inserters.spotify.get_insertions_manager(spotify_client)
        migrator = SpotifyTopTracksMigrator(spotify_client, insertions_manager)

        await migrator.migrate()


async def main():
    pre_processor = DataPreProcessor(max_year=2024)
    pre_processor.pre_process(output_path=MERGED_DATA_PATH)
    await DatabaseMigrator().migrate()
    await ShazamTopTracksMigrationScript().run(minimal_date=datetime(2023, 11, 27))
    await ShazamTracksMigrationScript().run()
    await run_spotify_top_tracks_migration()
    PlaylistsCreatorDatabaseGenerator().generate_database()


if __name__ == '__main__':
    EnvironmentManager().set_env_variables()
    asyncio.get_event_loop().run_until_complete(main())

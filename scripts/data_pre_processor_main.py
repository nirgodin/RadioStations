import asyncio
from datetime import datetime

from consts.path_consts import MERGED_DATA_PATH
from data_processing.data_pre_processor import DataPreProcessor
from database.migration_script import DatabaseMigrator
from database.shazam_top_tracks_migration_script import ShazamTopTracksMigrationScript
from database.shazam_tracks_migration_script import ShazamTracksMigrationScript
from research.playlists_creator_database.playlists_creator_database_generator import PlaylistsCreatorDatabaseGenerator
from tools.environment_manager import EnvironmentManager


async def main():
    await DatabaseMigrator().migrate()
    await ShazamTopTracksMigrationScript().run(minimal_date=datetime(2023, 11, 27))
    await ShazamTracksMigrationScript().run()


if __name__ == '__main__':
    EnvironmentManager().set_env_variables()

    pre_processor = DataPreProcessor(max_year=2023)
    pre_processor.pre_process(output_path=MERGED_DATA_PATH)
    asyncio.get_event_loop().run_until_complete(main())
    PlaylistsCreatorDatabaseGenerator().generate_database()

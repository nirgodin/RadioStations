import asyncio

from consts.path_consts import MERGED_DATA_PATH
from data_processing.data_pre_processor import DataPreProcessor
from database.migration_script import DatabaseMigrator
from research.playlists_creator_database.playlists_creator_database_generator import PlaylistsCreatorDatabaseGenerator
from tools.environment_manager import EnvironmentManager

if __name__ == '__main__':
    EnvironmentManager().set_env_variables()

    pre_processor = DataPreProcessor(max_year=2023)
    pre_processor.pre_process(output_path=MERGED_DATA_PATH)
    PlaylistsCreatorDatabaseGenerator().generate_database()
    asyncio.get_event_loop().run_until_complete(DatabaseMigrator().migrate())

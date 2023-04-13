import os.path
from typing import Dict

from consts.env_consts import ENV_VARIABLES_FILE_ID
from consts.path_consts import ENV_VARIABLES_FILE_PATH
from tools.google_drive.google_drive_adapter import GoogleDriveAdapter
from tools.google_drive.google_drive_download_metadata import GoogleDriveDownloadMetadata
from utils.file_utils import read_json


class EnvironmentManager:
    def __init__(self):
        self._google_drive_adapter = GoogleDriveAdapter()

    def set_env_variables(self) -> None:
        variables = self._read_env_variables_file()

        for k, v in variables.items():
            os.environ[k] = v
            print(f'Successfully set env variable `{k}`')

    def _read_env_variables_file(self) -> Dict[str, str]:
        if not os.path.exists(ENV_VARIABLES_FILE_PATH):
            print('Env variables data is missing. Downloading from remote')
            download_metadata = GoogleDriveDownloadMetadata(
                file_id=os.environ[ENV_VARIABLES_FILE_ID],
                local_path=ENV_VARIABLES_FILE_PATH
            )
            self._google_drive_adapter.download([download_metadata])

        return read_json(ENV_VARIABLES_FILE_PATH)

from dataclasses import dataclass
from typing import Optional

from utils.datetime_utils import get_current_datetime


@dataclass
class GoogleDriveFileMetadata:
    local_path: str
    drive_folder_id: str
    file_name: Optional[str] = None

    def __post_init__(self):
        self.metadata = {
            'name': self._get_file_name(),
            'parents': [self.drive_folder_id]
        }

    def _get_file_name(self) -> str:
        if self.file_name is not None:
            return self.file_name

        return get_current_datetime()

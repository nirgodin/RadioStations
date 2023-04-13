from dataclasses import dataclass
from typing import Optional

from utils.datetime_utils import get_current_datetime
from utils.file_utils import get_path_suffix


@dataclass
class GoogleDriveUploadMetadata:
    local_path: str
    drive_folder_id: str
    file_name: Optional[str] = None

    def __post_init__(self):
        self._set_file_name()
        self.metadata = {
            'name': self.file_name,
            'parents': [self.drive_folder_id]
        }

    def _set_file_name(self) -> None:
        if self.file_name is not None:
            return

        file_name = get_current_datetime()
        file_suffix = get_path_suffix(self.local_path)

        self.file_name = f'{file_name}{file_suffix}'

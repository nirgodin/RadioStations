import os
from dataclasses import dataclass


@dataclass
class GoogleDriveFileMetadata:
    local_path: str
    drive_folder_id: str

    def __post_init__(self):
        self.metadata = {
            'name': os.path.basename(self.local_path),
            'parents': [self.drive_folder_id]
        }

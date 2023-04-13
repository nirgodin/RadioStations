from dataclasses import dataclass


@dataclass
class GoogleDriveDownloadMetadata:
    local_path: str
    file_id: str

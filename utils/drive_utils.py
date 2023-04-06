from tools.google_drive.google_drive_adapter import GoogleDriveAdapter
from tools.google_drive.google_drive_file_metadata import GoogleDriveFileMetadata
from utils.general_utils import is_remote_run


def upload_files_to_drive(*files_metadata: GoogleDriveFileMetadata) -> None:
    if is_remote_run():
        GoogleDriveAdapter().upload(files_metadata)
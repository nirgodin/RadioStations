import json
import os
from typing import Iterable

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

from tools.google_drive.google_drive_file_metadata import GoogleDriveFileMetadata


class GoogleDriveAdapter:
    def __init__(self):
        self._drive_service = build(
            serviceName='drive',
            version='v3',
            credentials=json.loads(os.environ['CREDENTIALS'])
        )

    def upload(self, files_metadata: Iterable[GoogleDriveFileMetadata]) -> None:
        for file in files_metadata:
            media = MediaFileUpload(file.local_path, resumable=True)

            try:
                file = self._drive_service.files().create(body=file.metadata, media_body=media, fields='id').execute()
                print(f'File ID: {file.get("id")} uploaded successfully')
            except HttpError as error:
                print(f'An error occurred: {error}')
            finally:
                media.stream().close()

import json
import os
from typing import Iterable

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload, BatchHttpRequest

from consts.env_consts import GOOGLE_SERVICE_ACCOUNT_CREDENTIALS
from consts.path_consts import SERVICE_ACCOUNT_SECRETS_PATH
from tools.google_drive.google_drive_download_metadata import GoogleDriveDownloadMetadata
from tools.google_drive.google_drive_upload_metadata import GoogleDriveUploadMetadata


class GoogleDriveAdapter:
    def __init__(self):
        self._drive_service = build(
            serviceName='drive',
            version='v3',
            credentials=self._build_credentials()
        )

    def download(self, files_metadata: Iterable[GoogleDriveDownloadMetadata]) -> None:
        for file in files_metadata:
            file_content = self._drive_service.files().get_media(fileId=file.file_id).execute()

            with open(file.local_path, 'wb') as f:
                f.write(file_content)

            print(f'Successfully downloaded file to {file.local_path}')

    def upload(self, files_metadata: Iterable[GoogleDriveUploadMetadata]) -> None:
        for file in files_metadata:
            media = MediaFileUpload(file.local_path, resumable=True)

            try:
                file = self._drive_service.files().create(body=file.metadata, media_body=media, fields='id').execute()
                print(f'File ID: {file.get("id")} uploaded successfully')
            except HttpError as error:
                print(f'An error occurred: {error}')
            finally:
                media.stream().close()

    def clean_folder(self, folder_id: str) -> None:
        query = f"'{folder_id}' in parents and trashed=false"
        results = self._drive_service.files().list(q=query).execute()
        files = results.get('files', [])
        batch = BatchHttpRequest()

        for file in files:
            batch.add(self._drive_service.files().delete(fileId=file['id']))

        batch.execute()

        print(f"All folder `{folder_id}` contents deleted successfully!")

    @staticmethod
    def _build_credentials() -> Credentials:
        if os.path.exists(SERVICE_ACCOUNT_SECRETS_PATH):
            return Credentials.from_service_account_file(SERVICE_ACCOUNT_SECRETS_PATH)

        elif GOOGLE_SERVICE_ACCOUNT_CREDENTIALS in os.environ.keys():
            credentials = json.loads(os.environ[GOOGLE_SERVICE_ACCOUNT_CREDENTIALS])
            return Credentials.from_service_account_info(credentials)

        else:
            raise ValueError('Missing Google service account credentials')

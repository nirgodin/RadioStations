import base64
import os

import requests

from consts.api_consts import TOKEN_REQUEST_URL


class AccessTokenGenerator:
    @staticmethod
    def generate():
        encoded_header = AccessTokenGenerator._get_encoded_header()
        headers = {'Authorization': f"Basic {encoded_header}"}
        data = {
            'grant_type': 'client_credentials',
            'json': True
        }
        response = requests.post(
            url=TOKEN_REQUEST_URL,
            headers=headers,
            data=data
        ).json()

        return response['access_token']

    @staticmethod
    def _get_encoded_header():
        client_id = os.environ['SPOTIPY_CLIENT_ID']
        client_secret = os.environ['SPOTIPY_CLIENT_SECRET']
        bytes_auth = bytes(f"{client_id}:{client_secret}", "ISO-8859-1")
        b64_auth = base64.b64encode(bytes_auth)

        return b64_auth.decode('ascii')

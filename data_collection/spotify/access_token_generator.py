import base64
import os
import webbrowser
from typing import Iterable, Dict
from urllib.parse import urlencode

import requests
from spotipy.oauth2 import start_local_http_server

from consts.api_consts import TOKEN_REQUEST_URL, CLIENT_ID, RESPONSE_TYPE, REDIRECT_URI, CODE, SCOPE, \
    AUTHORIZATION_REQUEST_URL_FORMAT, GRANT_TYPE, JSON, ACCESS_TOKEN
from consts.env_consts import SPOTIPY_CLIENT_SECRET, SPOTIPY_CLIENT_ID, SPOTIPY_REDIRECT_URI, SPOTIPY_PORT
from data_collection.spotify.spotify_grant_type import SpotifyGrantType
from data_collection.spotify.spotify_scope import SpotifyScope


class AccessTokenGenerator:
    @staticmethod
    def generate(is_authorization_token: bool, scopes: Iterable[SpotifyScope]) -> str:
        encoded_header = AccessTokenGenerator._get_encoded_header()
        headers = {'Authorization': f"Basic {encoded_header}"}
        data = AccessTokenGenerator._build_request_payload(is_authorization_token, scopes)
        response = requests.post(
            url=TOKEN_REQUEST_URL,
            headers=headers,
            data=data
        ).json()

        return response[ACCESS_TOKEN]

    @staticmethod
    def _get_encoded_header() -> str:
        client_id = os.environ[SPOTIPY_CLIENT_ID]
        client_secret = os.environ[SPOTIPY_CLIENT_SECRET]
        bytes_auth = bytes(f"{client_id}:{client_secret}", "ISO-8859-1")
        b64_auth = base64.b64encode(bytes_auth)

        return b64_auth.decode('ascii')

    @staticmethod
    def _build_request_payload(is_authorization_token: bool, scopes: Iterable[SpotifyScope]) -> Dict[str, str]:
        if not is_authorization_token:
            return {
                GRANT_TYPE: SpotifyGrantType.CLIENT_CREDENTIALS.value,
                JSON: True
            }

        auth_code = AccessTokenGenerator._authorize(scopes)

        return {
            GRANT_TYPE: SpotifyGrantType.AUTHORIZATION_CODE.value,
            CODE: auth_code,
            REDIRECT_URI: os.environ[SPOTIPY_REDIRECT_URI],
            JSON: True
        }

    @staticmethod
    def _authorize(scopes: Iterable[SpotifyScope]) -> str:
        auth_url = AccessTokenGenerator._build_authorization_url(scopes)
        port = AccessTokenGenerator._get_port()
        server = start_local_http_server(port)
        webbrowser.open(auth_url)
        server.handle_request()

        return server.auth_code

    @staticmethod
    def _build_authorization_url(scopes: Iterable[SpotifyScope]) -> str:
        formatted_scopes = [scope.value for scope in scopes]
        auth_params = {
            CLIENT_ID: os.environ[SPOTIPY_CLIENT_ID],
            RESPONSE_TYPE: CODE,
            REDIRECT_URI: os.environ[SPOTIPY_REDIRECT_URI],
            SCOPE: ' '.join(formatted_scopes)
        }
        encoded_params = urlencode(auth_params)

        return AUTHORIZATION_REQUEST_URL_FORMAT.format(encoded_params)

    @staticmethod
    def _get_port() -> int:
        return int(os.environ[SPOTIPY_PORT])

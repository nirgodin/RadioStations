import base64
import os
import webbrowser
from typing import Iterable
from urllib.parse import urlencode

import requests
from spotipy.oauth2 import start_local_http_server

from consts.api_consts import CLIENT_ID, RESPONSE_TYPE, REDIRECT_URI, SCOPE, AUTHORIZATION_REQUEST_URL_FORMAT, \
    TOKEN_REQUEST_URL
from consts.env_consts import SPOTIPY_CLIENT_ID, SPOTIPY_REDIRECT_URI, SPOTIPY_PORT, SPOTIPY_CLIENT_SECRET
from data_collection.spotify.spotify_scope import SpotifyScope
from utils.spotify_utils import get_port


class SpotifyAuthorizer:
    @staticmethod
    def generate_access_token(scopes: Iterable[SpotifyScope]) -> str:
        auth_code = SpotifyAuthorizer._authorize(scopes)
        encoded_header = SpotifyAuthorizer._get_encoded_header()
        headers = {'Authorization': f"Basic {encoded_header}"}
        data = {
            'grant_type': 'authorization_code',
            'code': auth_code,
            'redirect_uri': os.environ[SPOTIPY_REDIRECT_URI],
            'json': True
        }
        response = requests.post(
            url=TOKEN_REQUEST_URL,
            headers=headers,
            data=data
        ).json()

        return response['access_token']

    @staticmethod
    def _authorize(scopes: Iterable[SpotifyScope] = ()) -> str:
        auth_url = SpotifyAuthorizer._build_url(scopes)
        port = get_port()
        server = start_local_http_server(port)
        webbrowser.open(auth_url)
        server.handle_request()

        return server.auth_code

    @staticmethod
    def _build_url(scopes: Iterable[SpotifyScope]) -> str:
        formatted_scopes = [scope.value for scope in scopes]
        auth_params = {
            CLIENT_ID: os.environ[SPOTIPY_CLIENT_ID],
            RESPONSE_TYPE: 'code',
            REDIRECT_URI: os.environ[SPOTIPY_REDIRECT_URI],
            SCOPE: ' '.join(formatted_scopes)
        }
        encoded_params = urlencode(auth_params)

        return AUTHORIZATION_REQUEST_URL_FORMAT.format(encoded_params)

    @staticmethod
    def _get_encoded_header():
        client_id = os.environ[SPOTIPY_CLIENT_ID]
        client_secret = os.environ[SPOTIPY_CLIENT_SECRET]
        bytes_auth = bytes(f"{client_id}:{client_secret}", "ISO-8859-1")
        b64_auth = base64.b64encode(bytes_auth)

        return b64_auth.decode('ascii')

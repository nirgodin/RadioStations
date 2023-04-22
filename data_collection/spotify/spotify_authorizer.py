import os
import webbrowser
from typing import Iterable
from urllib.parse import urlencode

from spotipy.oauth2 import start_local_http_server

from consts.api_consts import CLIENT_ID, RESPONSE_TYPE, REDIRECT_URI, SCOPE, AUTHORIZATION_REQUEST_URL_FORMAT
from consts.env_consts import SPOTIPY_CLIENT_ID, SPOTIPY_REDIRECT_URI, SPOTIPY_PORT
from data_collection.spotify.spotify_scope import SpotifyScope


class SpotifyAuthorizer:
    def authorize(self, scopes: Iterable[SpotifyScope] = ()) -> None:
        auth_url = self._build_url(scopes)
        port = os.environ[SPOTIPY_PORT]
        start_local_http_server(port)

        webbrowser.open(auth_url)

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

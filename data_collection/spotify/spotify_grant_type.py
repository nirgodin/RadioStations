from enum import Enum


class SpotifyGrantType(Enum):
    CLIENT_CREDENTIALS = 'client_credentials'
    AUTHORIZATION_CODE = 'authorization_code'

from enum import Enum


class DataSource(Enum):
    # TODO: Add prioritization as part of Enum? (manual_tagging, 1)
    MANUAL_TAGGING = "manual_tagging"
    OPENAI = "openai"
    ISRAELI_WIKIPEDIA = "israeli_wikipedia"
    GENERAL_WIKIPEDIA = "general_wikipedia"
    SPOTIFY_EQUAL_PLAYLISTS = "spotify_equal_playlists"
    GOOGLE_IMAGES = "google_images"

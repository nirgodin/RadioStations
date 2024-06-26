from consts.data_consts import MAIN_GENRE, POPULARITY, EXPLICIT, DURATION_MS, IS_ISRAELI, TRACK_NUMBER, RELEASE_DATE, \
    ID, TYPE, TRACK_HREF, ANALYSIS_URL, ERROR

DANCEABILITY = 'danceability'
ENERGY = 'energy'
KEY = 'key'
LOUDNESS = 'loudness'
MODE = 'mode'
SPEECHINESS = 'speechiness'
ACOUSTICNESS = 'acousticness'
INSTRUMENTALNESS = 'instrumentalness'
LIVENESS = 'liveness'
VALENCE = 'valence'
TEMPO = 'tempo'

AUDIO_FEATURES = [
    ACOUSTICNESS,
    DANCEABILITY,
    ENERGY,
    INSTRUMENTALNESS,
    KEY,
    LIVENESS,
    LOUDNESS,
    MODE,
    SPEECHINESS,
    TEMPO,
    VALENCE
]

KEY_NAMES_MAPPING = {
    0: 'C',
    1: 'C#',
    2: 'D',
    3: 'D#',
    4: 'E',
    5: 'F',
    6: 'F#',
    7: 'G',
    8: 'G#',
    9: 'A',
    10: 'A#',
    11: 'B'
}
REVERSED_KEYS_MAPPING = {v: k for k, v in KEY_NAMES_MAPPING.items()}
DUMMY_COLUMNS = [
    KEY,
    MAIN_GENRE
]
PLAYS_COUNT = 'play_count'
CORRELATION_COLUMNS_SUBSET = AUDIO_FEATURES + [
    POPULARITY,
    EXPLICIT,
    DURATION_MS,
    IS_ISRAELI,
    TRACK_NUMBER,
    RELEASE_DATE,
    ID,
    MAIN_GENRE
]
CORRELATION = 'correlation'
X = 'x'
Y = 'y'

MERGED_DATA_DTYPES = {
    KEY: str,
    TYPE: str,
    TRACK_HREF: str,
    ANALYSIS_URL: str,
    ERROR: str
}

from consts.aggregation_consts import FIRST, SUM
from consts.audio_features_consts import KEY, DANCEABILITY, ENERGY, LOUDNESS, MODE, SPEECHINESS, ACOUSTICNESS, \
    INSTRUMENTALNESS, LIVENESS, VALENCE, TEMPO
from consts.data_consts import SONG, ARTIST_ID, ERROR, ANALYSIS_URL, TRACK_HREF, URI, ID, TYPE, SCRAPED_AT, SNAPSHOT_ID, \
    STATION, RELEASE_DATE, MAIN_ALBUM, MAIN_GENRE, GENRES, ARTIST_NAME, ADDED_AT, NAME, FOLLOWERS, BROADCASTING_YEAR, \
    POPULARITY, TRACK_NUMBER, DURATION_MS, EXPLICIT, ARTIST_FOLLOWERS, ARTIST_POPULARITY, ALBUM_TRACKS_NUMBER, \
    IS_ISRAELI, TIME_SIGNATURE
from consts.gender_consts import SOURCE
from consts.language_consts import LANGUAGE, SCORE
from consts.openai_consts import ARTIST_GENDER
from consts.spotify_albums_details_consts import MEDIAN_MARKETS_NUMBER, FIRST_ALBUM_RELEASE_YEAR, \
    LAST_ALBUM_RELEASE_YEAR, YEARS_ACTIVE, APPEARS_ON_SINGLES_COUNT, APPEARS_ON_COMPILATIONS_COUNT, \
    APPEARS_ON_ALBUMS_COUNT, SINGLES_COUNT, COMPILATIONS_COUNT, ALBUMS_COUNT
from data_processing.pre_processors.language_pre_processor import SHAZAM_KEY, SHAZAM_ADAMID

DROPPABLE_COLUMNS = [
    MEDIAN_MARKETS_NUMBER,
    MEDIAN_MARKETS_NUMBER,
    FIRST_ALBUM_RELEASE_YEAR,
    LAST_ALBUM_RELEASE_YEAR,
    YEARS_ACTIVE,
    ALBUMS_COUNT,
    COMPILATIONS_COUNT,
    SINGLES_COUNT,
    APPEARS_ON_ALBUMS_COUNT,
    APPEARS_ON_COMPILATIONS_COUNT,
    APPEARS_ON_SINGLES_COUNT,
    SCORE,
    RELEASE_DATE,
    ADDED_AT,
    SNAPSHOT_ID,
    SCRAPED_AT,
    TYPE,
    ID,
    TRACK_HREF,
    ANALYSIS_URL,
    ERROR,
    ARTIST_ID,
    SHAZAM_KEY,
    SHAZAM_ADAMID,
    BROADCASTING_YEAR,
    SOURCE,
    STATION
]

GROUPBY_COLUMNS = [
    SONG
]

GROUPBY_FIRST_COLUMNS = [
    LANGUAGE,
    KEY,
    MAIN_GENRE,
    ARTIST_GENDER,
    URI,
    NAME,
    ARTIST_NAME,
    GENRES,
    MAIN_ALBUM,
    TRACK_NUMBER,
    DURATION_MS,
    EXPLICIT,
    ALBUM_TRACKS_NUMBER,
    IS_ISRAELI,
    DANCEABILITY,
    ENERGY,
    LOUDNESS,
    MODE,
    SPEECHINESS,
    ACOUSTICNESS,
    INSTRUMENTALNESS,
    LIVENESS,
    VALENCE,
    TEMPO,
    TIME_SIGNATURE,
]

GROUPBY_MEDIAN_COLUMNS = [
    POPULARITY,
    ARTIST_FOLLOWERS,
    ARTIST_POPULARITY
]

LINEAR_TRANSFORMED_COLUMNS = [
    DANCEABILITY,
    ENERGY,
    SPEECHINESS,
    ACOUSTICNESS,
    INSTRUMENTALNESS,
    LIVENESS,
    VALENCE
]
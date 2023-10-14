from consts.data_consts import ALBUM_GROUP, ALBUM_TYPE, ID, ARTIST_ID, NAME, RELEASE_DATE, RELEASE_DATE_PRECISION, \
    TOTAL_TRACKS, MAIN_ALBUM, ALBUM_ID, ALBUM_RELEASE_DATE

NAMED_ALBUMS_OPTIONS = {
    'albums_count': ('album', 'album'),
    'compilations_count': ('compilation', 'compilation'),
    'singles_count': ('single', 'single'),
    'appears_on_albums_count': ('appears_on', 'album'),
    'appears_on_compilations_count': ('appears_on', 'compilation'),
    'appears_on_singles_count': ('appears_on', 'single')
}
AVAILABLE_MARKETS = 'available_markets'
AVAILABLE_MARKETS_NUMBER = 'available_markets_number'
MEDIAN_MARKETS_NUMBER = 'median_markets_number'
ALBUM_RELEASE_YEAR = 'album_release_year'
FIRST_ALBUM_RELEASE_YEAR = 'first_album_release_year'
LAST_ALBUM_RELEASE_YEAR = 'last_album_release_year'
YEARS_ACTIVE = 'years_active'
ALBUMS_COUNT = 'albums_count'
COMPILATIONS_COUNT = 'compilations_count'
SINGLES_COUNT = 'singles_count'
APPEARS_ON_ALBUMS_COUNT = 'appears_on_albums_count'
APPEARS_ON_COMPILATIONS_COUNT = 'appears_on_compilations_count'
APPEARS_ON_SINGLES_COUNT = 'appears_on_singles_count'
RAW_ALBUMS_DETAILS_RELEVANT_COLUMNS = [
    ALBUM_GROUP,
    ALBUM_TYPE,
    ID,
    ARTIST_ID,
    NAME,
    RELEASE_DATE,
    RELEASE_DATE_PRECISION,
    TOTAL_TRACKS
]
ALBUMS_COLUMNS_RENAME_MAPPING = {
    NAME: MAIN_ALBUM,
    ID: ALBUM_ID,
    RELEASE_DATE: ALBUM_RELEASE_DATE
}
RAW_ALBUMS_DETAILS_MERGE_COLUMNS = [ARTIST_ID, MAIN_ALBUM]
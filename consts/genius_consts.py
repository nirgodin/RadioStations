from consts.data_consts import ID

GENIUS_API_BASE_URL = 'https://api.genius.com'
GENIUS_API_SEARCH_URL = f'{GENIUS_API_BASE_URL}/search'
GENIUS_API_SONG_URL_FORMAT = f'{GENIUS_API_BASE_URL}/songs/{{}}'
GENIUS_LYRICS_URL_FORMAT = 'https://genius.com/{}'
META = 'meta'
STATUS = 'status'
RESPONSE = 'response'
RESULT = 'result'
PATH = 'path'

IRRELEVANT_SONG_COLLECTOR_KEYS = [
    'annotation_count',
    'api_path',
    'artist_names',
    'full_title',
    'header_image_thumbnail_url',
    'header_image_url',
    ID,
    'lyrics_owner_id',
    'lyrics_state',
    PATH,
    'pyongs_count',
    'relationships_index_url',
    'release_date_for_display',
    'release_date_with_abbreviated_month_for_display',
    'song_art_image_thumbnail_url',
    'song_art_image_url',
    'title',
    'title_with_featured',
    'url',
    'featured_artists',
    'primary_artist'
]
DATA_LYRICS_CONTAINER = 'data-lyrics-container'
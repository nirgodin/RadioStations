from consts.data_consts import ARTIST_NAME, NAME, ID, POPULARITY

MUSIXMATCH_TRACK_SEARCH_URL_FORMAT = 'http://api.musixmatch.com/ws/1.1/track.search?q_artist={}&q_track={}&apikey={}'
MUSIXMATCH_LYRICS_URL_FORMAT = 'http://api.musixmatch.com/ws/1.1/track.lyrics.get?track_id={}&apikey={}'
MUSIXMATCH_API_KEY = 'MUSIXMATCH_API_KEY'
DAILY_REQUESTS_LIMIT = 2000
MUSIXMATCH_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json"
}
MUSIXMATCH_TRACK_ID = 'track_id'
MESSAGE = 'message'
HEADER = 'header'
STATUS_CODE = 'status_code'
OK_STATUS_CODE = 200
BODY = 'body'
TRACK_LIST = 'track_list'
LYRICS = 'lyrics'
LYRICS_BODY = 'lyrics_body'
TRACK_NAME = 'track_name'
TRACK_RATING = 'track_rating'

MUSIXMATCH_RELEVANT_COLUMNS = [
    ARTIST_NAME,
    NAME,
    ID,
    POPULARITY
]
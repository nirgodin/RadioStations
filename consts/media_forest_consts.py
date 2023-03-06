from consts.data_consts import ARTIST_NAME, NAME

MEDIA_FOREST_URL = 'https://mediaforest-group.com/weekly_charts.html'
MEDIA_FOREST_WEEKLY_CHARTS_XPATH_FORMAT = '//*[@id="ntttab"]/li[{}]'
MEDIA_FOREST_MENUS_MAPPING = {
    '1': 'top_israeli_songs',
    '2': 'top_international_songs',
    '3': 'top_israeli_artists',
    '4': 'top_international_artists'
}
PHOTOS_COLUMN_NAME = 'Unnamed: 1'
DAY_NIGHT = 'day/night'
DAY = 'day'
NIGHT = 'night'
LEAD_BY = 'lead by'
PLAY_COUNT = 'play_count'
STATION = 'station'
ARTIST_SONG_COLUMN_NAME = 'mediaforest'
RANK_ORIGINAL_COLUMN_NAME = 'Unnamed: 0'
RANK = 'rank'
SONGS_COLUMNS_SPLIT = {
    ARTIST_SONG_COLUMN_NAME: [ARTIST_NAME, NAME],
    DAY_NIGHT: [DAY, NIGHT],
    LEAD_BY: [STATION, PLAY_COUNT]
}
SONGS_RENAME_MAPPING = {RANK_ORIGINAL_COLUMN_NAME: RANK}
ARTISTS_COLUMNS_SPLIT = {
    DAY_NIGHT: [DAY, NIGHT],
}
ARTISTS_RENAME_MAPPING = {RANK_ORIGINAL_COLUMN_NAME: RANK, ARTIST_SONG_COLUMN_NAME: ARTIST_NAME}

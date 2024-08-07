# Directories
# Shazam
SHAZAM_DIR_PATH = r'data/shazam'
SHAZAM_ISRAEL_DIR_PATH = rf'{SHAZAM_DIR_PATH}/israel'
SHAZAM_WORLD_DIR_PATH = rf'{SHAZAM_DIR_PATH}/world'
SHAZAM_CITIES_DIR_PATH = rf'{SHAZAM_DIR_PATH}/cities'

# Spotify
SPOTIFY_DIR_PATH = r'data/spotify'
RADIO_STATIONS_SNAPSHOTS_DIR = rf'{SPOTIFY_DIR_PATH}/radio_stations_snapshots'

# Analyzers
ANALYSIS_OUTPUTS_DIR_PATH = r'analysis/analysis_outputs'

# Genius
GENIUS_DIR_PATH = r'data/genius'

# Musixmatch
MUSIXMATCH_DIR_PATH = r'data/musixmatch'

# Files
# Shazam
SHAZAM_CITIES_PATH_FORMAT = rf'{SHAZAM_CITIES_DIR_PATH}/{{}}.csv'
SHAZAM_ISRAEL_PATH_FORMAT = rf'{SHAZAM_ISRAEL_DIR_PATH}/{{}}.csv'
SHAZAM_WORLD_PATH_FORMAT = rf'{SHAZAM_WORLD_DIR_PATH}/{{}}.csv'
SHAZAM_TRACKS_IDS_PATH = rf'{SHAZAM_DIR_PATH}/tracks_ids.csv'
SHAZAM_LISTENING_COUNT_PATH = rf'{SHAZAM_DIR_PATH}/listening_count.csv'
SHAZAM_TRACKS_ABOUT_PATH = rf'{SHAZAM_DIR_PATH}/tracks_about.csv'
SHAZAM_TRACKS_LYRICS_PATH = rf'{SHAZAM_DIR_PATH}/tracks_lyrics.json'
SHAZAM_TRACKS_LANGUAGES_PATH = rf'{SHAZAM_DIR_PATH}/tracks_languages.csv'
SHAZAM_LYRICS_EMBEDDINGS_PATH = rf'{SHAZAM_DIR_PATH}/lyrics_embeddings.csv'
SHAZAM_ISRAEL_MERGED_DATA = rf'{SHAZAM_DIR_PATH}/israel_merged_data.csv'
SHAZAM_ARTISTS_IDS_PATH = rf'{SHAZAM_DIR_PATH}/artists_ids.csv'

# Spotify
ARTISTS_IDS_OUTPUT_PATH = rf'{SPOTIFY_DIR_PATH}/spotify_artists_ids.csv'
ALBUMS_DETAILS_OUTPUT_PATH = rf'{SPOTIFY_DIR_PATH}/spotify_albums_details.csv'
TRACKS_ALBUMS_DETAILS_OUTPUT_PATH = rf'{SPOTIFY_DIR_PATH}/spotify_tracks_albums_details.csv'
RADIO_STATIONS_PLAYLIST_SNAPSHOT_PATH_FORMAT = rf'{SPOTIFY_DIR_PATH}/radio_stations_snapshots/{{}}.csv'
SPOTIFY_ISRAELI_PLAYLISTS_OUTPUT_PATH = rf'{SPOTIFY_DIR_PATH}/israeli_playlists_artists.csv'
SPOTIFY_LGBTQ_PLAYLISTS_OUTPUT_PATH = rf'{SPOTIFY_DIR_PATH}/lgbtq_playlists_artists.csv'
TRACKS_IDS_OUTPUT_PATH = rf'{SPOTIFY_DIR_PATH}/spotify_tracks_ids.csv'
ARTISTS_UI_DETAILS_OUTPUT_PATH = rf'{SPOTIFY_DIR_PATH}/spotify_artists_ui_details.csv'

# Analyzers
KAN_GIMEL_ANALYZER_OUTPUT_PATH = r'data/resources/kan_gimel_tracks_artists_and_albums.json'
UNIQUE_ANALYZER_OUTPUT_PATH_FORMAT = r'data\unique_{}_share.csv'
ALBUMS_DETAILS_ANALYZER_OUTPUT_PATH = rf'{ANALYSIS_OUTPUTS_DIR_PATH}/albums_details_analysis_output.csv'
SPOTIFY_ARTISTS_UI_ANALYZER_OUTPUT_PATH = rf'{ANALYSIS_OUTPUTS_DIR_PATH}/artists_ui_analysis_output.csv'
TRACK_IDS_MAPPING_ANALYZER_OUTPUT_PATH = rf'{ANALYSIS_OUTPUTS_DIR_PATH}/tracks_ids_mapping.csv'
SHAZAM_TRACKS_ABOUT_ANALYZER_OUTPUT_PATH = rf'{ANALYSIS_OUTPUTS_DIR_PATH}/shazam_tracks_about_analysis_output.csv'
SHAZAM_APPLE_TRACKS_IDS_MAPPING_OUTPUT_PATH = rf'{ANALYSIS_OUTPUTS_DIR_PATH}/shazam_apple_tracks_ids_mapping.csv'
MUSIXMATCH_FORMATTED_TRACKS_LYRICS_PATH = rf'{ANALYSIS_OUTPUTS_DIR_PATH}/musixmatch_formatted_tracks_lyrics.json'

# Genius
GENIUS_TRACKS_IDS_OUTPUT_PATH = rf'{GENIUS_DIR_PATH}/genius_tracks_ids.csv'
GENIUS_SONGS_OUTPUT_PATH = rf'{GENIUS_DIR_PATH}/genius_songs.json'
GENIUS_LYRICS_OUTPUT_PATH = rf'{GENIUS_DIR_PATH}/genius_lyrics.json'

# Musixmatch
MUSIXMATCH_TRACK_IDS_PATH = rf'{MUSIXMATCH_DIR_PATH}/track_ids.json'
MUSIXMATCH_TRACKS_LYRICS_PATH = rf'{MUSIXMATCH_DIR_PATH}/tracks_lyrics.json'
MUSIXMATCH_TRACKS_LANGUAGES_PATH = rf'{MUSIXMATCH_DIR_PATH}/tracks_languages.csv'

MERGED_DATA_PATH = r'data/merged_data.csv'
AUDIO_FEATURES_CHUNK_OUTPUT_PATH_FORMAT = r'data/audio_features/audio_features_chunks/{}.csv'
AUDIO_FEATURES_BASE_DIR = r'data/audio_features/audio_features_chunks'
AUDIO_FEATURES_DATA_PATH = r'data/audio_features/audio_features_merged_data.csv'
CORRELATIONS_DATA_PATH = r'data/popularity_play_count_correlations_data.csv'
GENRES_LABELS_PATH = r'data\resources\genres_labels.csv'
GENRES_MAPPING_PATH = r'data/resources/genres_mapping.csv'
SHAZAM_CHARTS_METADATA_PATH = r'data_collection/shazam/resources/charts_metadata.json'
OPENAI_GENDERS_PATH = r'data/genders/openai/artists_genders.csv'
TRACKS_WORD_COUNT_PATH = r'data/lyrics/tracks_word_count.json'
TRANSLATIONS_PATH = r'data/translations.csv'
WIKIPEDIA_GENDERS_PATH = r'data/genders/wikipedia/artists_genders.csv'
WIKIPEDIA_ISRAELI_ARTISTS_GENDER_PATH = r'data/genders/wikipedia/israeli_artists_genders.csv'
HEBREW_WORDS_TXT_FILE_PATH_FORMAT = r'data_collection/wikipedia/gender/resources/{}_hebrew_words.txt'
WIKIPEDIA_RELEVANT_CATEGORIES_PATH = r'data_collection/wikipedia/categories/resources/relevant_categories_words.txt'
WIKIPEDIA_CATEGORIES_PATH = r'data_collection\wikipedia\categories\resources\relevant_categories.json'
WIKIPEDIA_CATEGORIES_MEMBERS_PATH = r'data\wikipedia\categories_members.csv'
WIKIPEDIA_OPENAI_UNKNOWN_GENDERS_PATH = r'data\genders\wikipedia\openai_unknown_genders.csv'
FACE_MODEL_PATH = r'tools/image_detection/resources/res10_300x300_ssd_iter_140000_fp16.caffemodel'
FACE_MODEL_WEIGHTS_PATH = r'tools/image_detection/resources/deploy.prototxt.txt'
GENDER_MODEL_WEIGHTS_PATH = r'tools/image_detection/resources/deploy_gender.prototxt'
GENDER_MODEL_PATH = r'tools/image_detection/resources/gender_net.caffemodel'
GOOGLE_IMAGES_GENDER_PATH = r'data/genders/google_images/artists_genders.csv'
GOOGLE_IMAGES_RESOURCES_PATH = r'data_collection/google_images/resources'
ARTISTS_PHOTOS_DIR_PATH = r'data_collection/google_images/resources/artists_photos'
MEDIAFOREST_PATH_FORMAT = r'data/mediaforest/{}/{}.csv'
MAPPED_GENDERS_OUTPUT_PATH = r'data/genders/mapped_genders.csv'
SPOTIFY_WEEKLY_RUN_LATEST_EXECUTIONS = r'data_collection/spotify/weekly_run/resources/weekly_run_latest_executions.json'
MAKO_HIT_LIST_OUTPUT_PATH_FORMAT = r'data/mako_hit_list/{}.csv'
MAIN_DATA_DIR_PATH = r'data'
DATA_DVC_PATH = r'data.dvc'
SERVICE_ACCOUNT_SECRETS_PATH = r'service_account_secrets.json'
ENV_VARIABLES_FILE_PATH = r'env_variables.json'
SPOTIFY_EQUAL_PLAYLISTS_OUTPUT_PATH = r'data/genders/spotify/equal_playlists_artists.csv'
WIKIPEDIA_AGE_OUTPUT_PATH = r'data/wikipedia_artists_ages.csv'
PLAYLISTS_CREATOR_DATABASE_FILE_NAME = 'playlists_creator_database.csv'
PLAYLISTS_CREATOR_DATABASE_OUTPUT_PATH = rf'data/research/{PLAYLISTS_CREATOR_DATABASE_FILE_NAME}'
LYRICS_NERS_PATH = r'data/lyrics/lyrics_ners.csv'
LANGUAGES_ABBREVIATIONS_MAPPING_PATH = r'data/resources/languages_abbreviations_mapping.json'
TRACK_NAMES_EMBEDDINGS_FILE_NAME = 'track_names_embeddings.csv'
TRACK_NAMES_EMBEDDINGS_PATH = rf'data/{TRACK_NAMES_EMBEDDINGS_FILE_NAME}'
MANUAL_GENDERS_TAGGING_PATH = r'data/genders/manual_genders_tags.csv'
YEAR_PLAY_COUNT_DIFFERENCE_PATH = r'data/year_play_count_difference.csv'
SPOTIFY_ISRAEL_GLOBAL_EQUAL_PLAYLISTS_OUTPUT_PATH = r'data/genders/spotify/israel_global_equal_playlists_artists.csv'
HEBREW_MONTHS_MAPPING_PATH = r'data_collection/wikipedia/age/hebrew_months_mapping.json'

from consts.data_consts import ARTIST_ID
from consts.wikipedia_consts import WIKIPEDIA_LANGUAGE, WIKIPEDIA_NAME

ARTIST_ABOUT_CLICK_BUTTON_CSS_SELECTOR = '#main > div > div.ZQftYELq0aOsg6tPbVbV > div.jEMA2gVoLgPQqAFrPhFw > div.main-view-container > div.os-host.os-host-foreign.os-theme-spotify.os-host-resize-disabled.os-host-scrollbar-horizontal-hidden.main-view-container__scroll-node.os-host-transition.os-host-overflow.os-host-overflow-y > div.os-padding > div > div > div.main-view-container__scroll-node-child > main > section > div > div.BL__GuO2JsHMR6RgNfwY > div.contentSpacing > div.iKwGKEfAfW7Rkx2_Ba4E > div.kgR8s9v7IzY4G17ZtLbw > div > button'
ARTIST_PAGE_URL_FORMAT = "https://open.spotify.com/artist/{}"
INSTAGRAM = "Instagram"
INSTAGRAM_NAME = "instagram_name"
FACEBOOK = "Facebook"
FACEBOOK_NAME = "facebook_name"
TWITTER = "Twitter"
TWITTER_NAME = "twitter_name"
ARTISTS_UI_ANALYZER_OUTPUT_COLUMNS = [
    ARTIST_ID,
    WIKIPEDIA_LANGUAGE,
    WIKIPEDIA_NAME,
    INSTAGRAM_NAME,
    FACEBOOK_NAME,
    TWITTER_NAME
]

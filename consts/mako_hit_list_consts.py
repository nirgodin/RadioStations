from consts.data_consts import NAME, ARTIST_NAME
from tools.website_crawling.html_element import HTMLElement
from tools.website_crawling.web_element import WebElement

MAKO_HIT_LIST_URL_FORMAT = 'https://hitlist.mako.co.il/{}'
CURRENT_RANK = 'current_rank'
PREVIOUS_WEEK_RANK = 'previous_week_rank'
ALL_TIME_RECORD_RANK = 'all_time_record_rank'
OVERALL = 'overall'
INTERNATIONAL = 'international'
ISRAEL = 'israel'

MAKO_HIT_LIST_ROUTES = {
    OVERALL: '',
    INTERNATIONAL: INTERNATIONAL,
    ISRAEL: ISRAEL
}

MAKO_HIT_LIST_WEB_ELEMENTS = [
    WebElement(
        name=CURRENT_RANK,
        type=HTMLElement.DIV,
        class_='number--y7e0a',
        multiple=True,
        enumerate=False
    ),
    WebElement(
        name=NAME,
        type=HTMLElement.H3,
        class_='title--cUGHa',
        multiple=True,
        enumerate=False
    ),
    WebElement(
        name=ARTIST_NAME,
        type=HTMLElement.H4,
        class_='artist--W6fVp',
        multiple=True,
        enumerate=False
    ),
    WebElement(
        name=PREVIOUS_WEEK_RANK,
        type=HTMLElement.DIV,
        class_='prev-week--P8CJ7',
        multiple=True,
        enumerate=False,
        child_element=WebElement(
            name=PREVIOUS_WEEK_RANK,
            type=HTMLElement.P,
            class_='position-stats-value--Yg5e2',
            multiple=True,
            enumerate=False
        )
    ),
    WebElement(
        name=ALL_TIME_RECORD_RANK,
        type=HTMLElement.DIV,
        class_='all-time-record--rM8ja',
        multiple=True,
        enumerate=False,
        child_element=WebElement(
            name=ALL_TIME_RECORD_RANK,
            type=HTMLElement.P,
            class_='position-stats-value--Yg5e2',
            multiple=True,
            enumerate=False
        )
    )
]

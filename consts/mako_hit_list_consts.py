from consts.html_consts import DIV, HEADER_3, HEADER_4, PARAGRAPH
from data_collection.mako_hit_list.web_element import WebElement
from consts.data_consts import NAME, ARTIST_NAME

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
        element_type=DIV,
        class_name='number--y7e0a'
    ),
    WebElement(
        name=NAME,
        element_type=HEADER_3,
        class_name='title--cUGHa'
    ),
    WebElement(
        name=ARTIST_NAME,
        element_type=HEADER_4,
        class_name='artist--W6fVp'
    ),
    WebElement(
        name=PREVIOUS_WEEK_RANK,
        element_type=DIV,
        class_name='prev-week--P8CJ7',
        child_element=WebElement(
            name=PREVIOUS_WEEK_RANK,
            element_type=PARAGRAPH,
            class_name='position-stats-value--Yg5e2'
        )
    ),
    WebElement(
        name=ALL_TIME_RECORD_RANK,
        element_type=DIV,
        class_name='all-time-record--rM8ja',
        child_element=WebElement(
            name=ALL_TIME_RECORD_RANK,
            element_type=PARAGRAPH,
            class_name='position-stats-value--Yg5e2'
        )
    )
]

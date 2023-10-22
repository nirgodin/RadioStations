from contextlib import contextmanager
from time import sleep
from typing import Generator

from selenium.webdriver import Chrome
from webdriver_manager.chrome import ChromeDriverManager


def open_window(driver: Chrome, window_name: str) -> None:
    driver.execute_script(f"window.open('about:blank', '{window_name}');")


@contextmanager
def driver_session() -> Generator[Chrome, None, None]:
    driver = create_driver()

    try:
        yield driver
    finally:
        driver.quit()


def create_driver() -> Chrome:
    return Chrome(ChromeDriverManager().install())


def switch_window(driver: Chrome, window_name: str, sleep_time: int = 1) -> None:
    try:
        driver.switch_to.window(window_name)
        sleep(sleep_time)

    except:
        print(f"Failed to switch to window `{window_name}`. Ignoring.")

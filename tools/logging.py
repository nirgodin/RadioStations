from logging import basicConfig, getLogger, Logger, INFO


def get_logger() -> Logger:
    basicConfig(
        format="[%(threadName)s] %(asctime)s [%(levelname)s] %(message)s",
        level=INFO,
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    return getLogger("PlaylistsCreatorLogger")


logger = get_logger()

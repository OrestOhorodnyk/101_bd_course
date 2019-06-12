from .logger_conf import make_logger
from .config import LOG_FILE_PATH
LOGGER = make_logger(LOG_FILE_PATH, 'consumer_logger')
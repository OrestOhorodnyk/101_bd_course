import os

IS_IN_DOCKER = os.environ.get('DOCKER', False)

BASEDIR = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))

LOG_FILE_PATH = os.path.join(BASEDIR, 'logs/producer_log.txt')

CONFIGS = {
    'KAFKA_PORT': os.environ.get('KAFKA_PORT') or 9092,
    'KAFKA_ADDRESS': os.environ.get('KAFKA_SERVERS') or 'kafka',
}

TOPIC_NAME = 'POC_2019'

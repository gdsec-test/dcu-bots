import logging
import os
import json
import yaml
import requests

from logging.config import dictConfig
from settings import config_by_name
from util.iris import IrisDB
from ConfigParser import SafeConfigParser
from datetime import date

env = os.getenv('sysenv', 'dev')
app_settings = config_by_name[env]()

path = ''
value = os.getenv('LOG_CFG')
if value:
    path = value
if os.path.exists(path):
    with open(path, 'rt') as f:
        lconfig = yaml.safe_load(f.read())
    dictConfig(lconfig)
else:
    logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def write_to_slack(_endpoint, _channel, _report_count):
    saturday = 5
    sunday = 6

    if not (_report_count and _endpoint and _channel):
        return

    if date.today().weekday() in {saturday, sunday} and _report_count < 40:
        return

    message = 'Total number of tickets in the IRIS review queue: {}'.format(_report_count) + '\n'
    payload = {'payload': json.dumps({
        'channel': _channel,
        'username': 'ReviewQ BOT',
        'text': message,
        'icon_emoji': ':qbert:'
    })
    }
    requests.post(_endpoint, data=payload)


if __name__ == '__main__':
    iris_db = IrisDB(app_settings)
    report_count = iris_db.get_review_queue_count(app_settings.IRIS_GROUP_ID_CSA, app_settings.IRIS_SERVICE_ID_REVIEW)

    # Code to invoke the slack channel
    try:
        config_parser = SafeConfigParser()
        dir_path = os.path.dirname(os.path.realpath(__file__))
        config_parser.read('{}/review_queue_bot_settings.ini'.format(dir_path))
        settings = dict(config_parser.items(env))
        endpoint = settings.get('slack_url')
        channel = settings.get('slack_channel')
        write_to_slack(endpoint, channel, report_count)
    except Exception as e:
        logger.error('Error while accessing the slack configuration for {}. {}'.format(env, e.message))

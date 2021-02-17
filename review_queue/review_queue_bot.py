import json
import logging
import os
from configparser import ConfigParser
from datetime import date
from logging.config import dictConfig

import requests
import yaml
from settings import config_by_name
from util.iris import IrisDB


def write_to_slack(_url, _channel, _report_count):
    """
    :param _url:
    :param _channel:
    :param _report_count:
    :return: None
    """
    _saturday = 5
    _sunday = 6

    if not (_report_count and _url and _channel):
        return

    if date.today().weekday() in {_saturday, _sunday} and _report_count < 40:
        return

    _message = 'Total number of tickets in the IRIS review queue: {}\n'.format(_report_count)
    _payload = {
        'payload': json.dumps(
            {
                'channel': _channel,
                'username': 'ReviewQ BOT',
                'text': _message,
                'icon_emoji': ':qbert:'
            }
        )
    }
    requests.post(_url, data=_payload)


def read_config(_env):
    """
    Reads the configuration ini file for the env specific settings
    :param _env: string representing run environment
    :return: dict of configuration settings for the env
    """
    _config_parser = ConfigParser()
    _dir_path = os.path.dirname(os.path.realpath(__file__))
    _config_parser.read('{}/review_queue_bot_settings.ini'.format(_dir_path))
    return dict(_config_parser.items(_env))


def setup_logging():
    """
    Sets up logging
    :return: handle to the logger
    """
    try:
        _path = './logging.yaml'
        _value = os.getenv('LOG_CFG')
        if _value:
            _path = _value
        if _path and os.path.exists(_path):
            with open(_path, 'rt') as f:
                _l_config = yaml.safe_load(f.read())
            dictConfig(_l_config)
        else:
            logging.basicConfig(level=logging.INFO)
    except Exception:
        logging.basicConfig(level=logging.INFO)
    finally:
        return logging.getLogger(__name__)


if __name__ == '__main__':
    _run_env = os.getenv('sysenv', 'dev')
    _logger = setup_logging()

    # Code to invoke the slack channel
    try:
        _app_settings = config_by_name[_run_env]()
        _iris_db = IrisDB(_app_settings)
        _settings = read_config(_run_env)
        write_to_slack(_settings.get('slack_url'),
                       _settings.get('slack_channel'),
                       _iris_db.get_review_queue_count(_app_settings.IRIS_GROUP_ID_CSA,
                                                       _app_settings.IRIS_SERVICE_ID_REVIEW))
    except Exception as e:
        _logger.error('Error while accessing the slack configuration for {}. {}'.format(_run_env, e))

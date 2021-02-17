import datetime
import json
import logging
import os
import smtplib
import sys
import time
from configparser import ConfigParser
from email.mime.text import MIMEText

import requests
from redis import Redis
from requests import get

"""
    This should run every 10 minutes, and should only write to the slack channel once every 24 hours
    at 6am, providing the API is up and functioning.  If there are currently issues preventing the
    API from responding appropriately, then the program should write to the slack channel every 10
    minutes.  This program should also send an email to {ALL DCU ENGINEERS}@godaddy.com when the API is
    determined to be not functional

    Logging occurs in the file: ~root/apibot.log

    If you pass in a single argument 'debug', then the program will run with parameters which
    will work on Brett Berry's MAC

    Message should indicate whether external API failed at the gateway, at sso or at the endpoint (Redis)
    and if it is failing at any of those, then it should go and check the internal
    1. Ping the SSO URL
    2. Authenticate via PyAuth (change to SCRAPI-DOO apibot endpoint)
    3. When we GET the apibot, we need to process the return and provide a more helpful message

    We need a new dictionary, to contain ote and prod data that looks like this:

{
    'ote': {
        'int_url':'',
        'ext_url':'',
        'key_secret':'',
        'jwt':'',
        'sso_url':''
    },
    'prod':{
        'int_url':'',
        'ext_url':'',
        'key_secret':'',
        'jwt':'',
        'sso_url':''
    }
}
"""


class Payload:
    """
    The Payload class holds the payload data, with methods allowing the user to modify certain
    data member variables on the fly, alert the payload to slack, and email the payload to all
    DCU engineers
    """
    _from_addr = 'DoNotReply@ApiBot.godaddy.com'
    _username = 'API BOT'
    _thumbs_up = ':+1:'
    _thumbs_down = ':-1:'
    _icon = ''
    _text = ''

    def __init__(self, _slack_url, _logger, _debug):
        self._slack_url = _slack_url
        self._logger = _logger
        self._email_addresses = ['bxberry', 'abean']
        self._channel = '#queue_bot_test'
        if not _debug:
            self._email_addresses.append(['sneiswonger', 'chauser', 'pmcconnell', 'lhalvorson', 'ebenson',
                                          'spetersen', 'ssanfratello', 'agrover'])
            self._channel = '#dcu_alerts'

    def set_text(self, _env, _msg, _up=True):
        """
        :param _env: string
        :param _msg: string
        :param _up: boolean
        :return: None
        """
        self._icon = self._thumbs_down
        if _up:
            self._icon = self._thumbs_up

        self._text = '<!channel> {msg} for {env} Abuse API'.format(msg=_msg, env=_env)

    def _get_payload(self):
        """
        :return: dict
        """
        _d_payload = {
            'payload': json.dumps({
                'channel': self._channel,
                'username': self._username,
                'icon_emoji': self._icon,
                'text': self._text
            })
        }
        return _d_payload

    def alert_to_slack(self):
        """
        Prints the payload to a defined slack channel
        :return: None
        """
        _r = requests.post(self._slack_url, data=self._get_payload())
        self._logger.debug('Print to Slack response: ' + str(_r))

    def _email_payload(self):
        """
        :return: None
        """
        _msg = MIMEText(self._text)
        _msg['Subject'] = self._text
        _msg['From'] = self._from_addr

        try:
            _s = smtplib.SMTP('localhost')
            for _address in self._email_addresses:
                _to = '{}@godaddy.com'.format(_address)
                _msg['To'] = _to
                _s.sendmail(self._from_addr, _to, _msg.as_string())
            _s.quit()

        except Exception as e:
            self._logger.error('Error sending mail: ' + str(e))

    def report_message(self, _message):
        """
        :param _message: string
        :return: None
        """
        self._logger.error(_message)
        self.alert_to_slack()
        self._email_payload()


class Health:
    _endpoint_data = None
    _error_flag = None
    FAILURE_THRESHOLD = 0.7  # only send alerts when failure rate is 70% or greater
    HEADERS = {'Accept': 'application/json', 'Authorization': 'sso-key {key}'}
    MESSAGES = {
        'api': 'FATAL: Abuse API DOWN for {env} - {a} out of {b} attempts failed - Reason: {reason}',
        'sso': 'WARNING: {env} SSO Failed in {a} out of {b} attempts - Reason: {reason}'
    }
    RECHECK_SSO_FAILURE = 10  # num of times to re-check sso after a failure
    RECHECK_EXT_GET = 10  # '                      ' external url
    RECHECK_INT_GET = 10  # '                      ' internal url
    RECHECK_SSO_DELAY = 1  # sleep time in seconds for sso re-check
    RECHECK_CURL_DELAY = 5  # sleep time in seconds for endpoint re-check
    _run_env = None
    _timeout = 5  # Time to wait for a GET request

    def __init__(self, _payload, _redis, _logger, _debug=False):
        self._payload = _payload
        self._redis = _redis
        self._logger = _logger

    def set_run_env(self, _run_env, _endpoint_data):
        """
        :params _run_env: string
        :params _endpoint_data: dict
        :return: None
        """
        self._run_env = _run_env
        self._endpoint_data = _endpoint_data
        self._error_flag = 'error_{}'.format(_run_env)

    def _set_error(self, _message):
        """
        Set the error flag in Redis and alert via Slack & email
        :param _message: string
        :return: None
        """
        try:
            _cur_val = self._redis.get(self._error_flag)
            if not _cur_val:
                self._payload.set_text(self._run_env, _message, False)
                self._payload.report_message(_message)

                # Set Redis error flag key
                self._redis.set(self._error_flag, '1')
        except Exception as e:
            self._logger.error(e)

    def _recover_error(self):
        """
        Clear the error flag in Redis and alert via Slack
        :return: None
        """

        # Check Redis for error flag key, if error flag key, alert
        if self._redis.get(self._error_flag) == b'1':
            # SLACK the fact that the API recovered
            self._payload.set_text(self._run_env, 'API RECOVERED')
            self._payload.alert_to_slack()

            # Clear Redis error flag key
            self._redis.delete(self._error_flag)

        else:
            # If No Redis error flag key, check current time, alert if current time is between 6:00am & 6:09am
            _six = datetime.datetime.strptime('06:00', '%H:%M')
            _six_nine = datetime.datetime.strptime('06:09', '%H:%M')
            _cur_time = datetime.datetime.now()

            if _six.time() <= _cur_time.time() <= _six_nine.time():
                self._payload.set_text(self._run_env, 'API OK')
                self._payload.alert_to_slack()

        # Clear Redis error flag key
        self._redis.delete(self._error_flag)

    def _check_sso(self, _session):
        """
        :param _session:
        :return: boolean
        """
        self._logger.debug('In _check_sso: {}'.format(self._run_env))
        # Stop bot from breaking on ote check
        if self._run_env == 'ote':
            return True

        # Check to see if the SSO is accessible
        try:
            # Stagger the re-checks by RECHECK_TIME_DELAY seconds
            time.sleep(self.RECHECK_SSO_DELAY)
            _status = _session.get(self._endpoint_data['sso_url'])
            return _status.status_code == 200

        except Exception as e:
            self._logger.error('Error running {}:{}'.format(self._endpoint_data['sso_url'], str(e)))
            return False

    def _get_endpoint(self, _url, _headers, _sleep=0):
        """
        Performs a get on the url provided to see if the health endpoint is responding
        :param _url: string
        :param _headers: dict
        :param _sleep: int optional
        :return: boolean
        """
        try:
            self._logger.debug('In _get_endpoint: {}/{}'.format(self._run_env, _url))

            # Stagger the re-checks by RECHECK_TIME_DELAY seconds
            time.sleep(_sleep)

            _r = get(_url, headers=_headers, timeout=self._timeout)
            _message = 'Status:{}, Output:{}'.format(_r.status_code, _r.text)
            if _r.status_code not in [200]:
                self._logger.error(_message)
                return False
            self._logger.info(_message)

            # CURLing endpoint was successful, health is successful if text ends with 'ok'
            return _r.text[-2:] == 'ok'

        except Exception as e:
            self._logger.error('ERROR: {}: Unknown Error running "{}": {}'.format(self._run_env, _url, e))
            return False

    def _get_failed_runs(self, _func, _params, _recheck, _type, _reason):
        """
        Check an endpoint repeatedly to count and return the number of failures
        :param _func: function to run
        :param _params: map parameters
        :param _recheck: int number of times to re-check
        :param _type: string: sso or api
        :param _reason: string reason for the failure
        :return: tuple (int, string)
        """

        # Dynamically build params to pass to map
        _map_params = []
        for _param in _params:
            _map_params.append([_param] * _recheck)  # Yes, each param needs to be multiplied as a list

        # Failure getting external endpoint, so re-run the get X number of times using map
        _map_list = list(map(_func, *_map_params))

        # Count the number of times False appears in the list
        _failed_runs = _map_list.count(False)
        _message = self.MESSAGES.get(_type).format(env=self._run_env,
                                                   a=_failed_runs,
                                                   b=_recheck,
                                                   reason=_reason)

        return _failed_runs, _message

    def check_health(self):
        """
        Check the SSO URL to see if it is responsive
        :return: None
        """
        with requests.sessions.Session() as session:
            if not self._check_sso(session):
                # SSO endpoint failed.  Re-run RECHECK_SSO_FAILURE number of times using map
                _failed_runs, _message = self._get_failed_runs(_func=self._check_sso,
                                                               _params=[session],
                                                               _recheck=self.RECHECK_SSO_FAILURE,
                                                               _type='sso',
                                                               _reason='Unable to communicate with SSO URL')

                if (_failed_runs / self.RECHECK_SSO_FAILURE) >= self.FAILURE_THRESHOLD:
                    self._payload.set_text(self._run_env, _message, False)
                    self._payload.report_message(_message)

        # Curl the External API endpoint
        _headers = self.HEADERS.copy()
        _headers['Authorization'] = _headers.get('Authorization', '').format(key=self._endpoint_data['key_secret'])
        if self._get_endpoint(self._endpoint_data['ext_url'], _headers):

            # Success curling external url endpoint, so recover if a previous failure exists
            self._recover_error()

        else:
            # Failure getting external endpoint, so re-run the get X number of times using map
            _param_list = [self._endpoint_data['ext_url'], _headers, self.RECHECK_CURL_DELAY]
            _failed_runs, _prev_msg = self._get_failed_runs(_func=self._get_endpoint,
                                                            _params=_param_list,
                                                            _recheck=self.RECHECK_EXT_GET,
                                                            _type='api',
                                                            _reason='EXT Endpoint Not Responding / Check SSO')

            if (_failed_runs / self.RECHECK_EXT_GET) >= self.FAILURE_THRESHOLD:
                _message = _prev_msg

                if not self._get_endpoint(self._endpoint_data['int_url'], _headers, self.RECHECK_CURL_DELAY):
                    # External endpoint is degraded, so get internal endpoint
                    _param_list = [self._endpoint_data['int_url'], _headers, self.RECHECK_CURL_DELAY]
                    _failed_runs, _prev_msg = self._get_failed_runs(_func=self._get_endpoint,
                                                                    _params=_param_list,
                                                                    _recheck=self.RECHECK_INT_GET,
                                                                    _type='api',
                                                                    _reason='INT Endpoint Not Responding / Check REDIS')

                    if (_failed_runs / self.RECHECK_INT_GET) >= self.FAILURE_THRESHOLD:
                        _message = _prev_msg

                # Alert the endpoint response degradation
                self._set_error(_message)

            else:
                self._recover_error()


def iterate_env():
    """
    A function to cycle through the endpoint_data dictionary and process each environment separately.
    We use redis to archive the error flag, so we know if we've recovered from an issue
    :return: None
    """
    # SET UP LOGGING
    logging.basicConfig(filename='apibot.log',
                        level=logging.INFO,
                        format='[%(levelname)s:%(asctime)s:%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s'
                        )
    _logger = logging.getLogger(__name__)

    # CHECK FOR DEBUG FLAG
    _debug = False
    if len(sys.argv) > 1:
        if sys.argv[1].lower() == 'debug':
            _debug = True
            _logger.setLevel(logging.DEBUG)
            _logger.debug('Debug is ON')

    _logger.info('Starting...')

    configp = ConfigParser()
    _endpoint_data = {}
    try:
        # Read configuration file
        _dir_path = os.path.dirname(os.path.realpath(__file__))
        configp.read('{}/api_bot_settings.ini'.format(_dir_path))

        # This is the environment specific data
        _endpoint_data = {
            'ote': dict(configp.items('ote')),
            'prod': dict(configp.items('prod')),
        }
        if _debug:
            _endpoint_data['dev'] = dict(configp.items('dev'))
    except Exception as e:
        _logger.fatal('Configuration error: {}'.format(e))

    _redis = None
    try:
        _redis = Redis(host='localhost',
                       port=6379,
                       password=_endpoint_data.get('prod', {}).get('redis_password'))
    except Exception as e:
        _logger.fatal('Cant connect to Redis: ' + str(e))

    _payload = Payload(configp.get('slack', 'slack_url'), _logger, _debug)
    _health = Health(_payload, _redis, _logger, _debug)

    for _run_env, _env_dict in _endpoint_data.items():
        """If need to turn off OTE bot, uncomment the if key ==ote code/continue block below"""
        # if _key == 'ote':
        #     continue
        if time.strftime('%a').startswith('S') and _run_env != 'prod':
            """ DO NOT RUN ANYTHING BUT PROD ON WEEKENDS """
            continue
        _logger.debug('Checking {} environment'.format(_run_env))
        _health.set_run_env(_run_env, _env_dict)
        _health.check_health()

    _logger.info('Finishing...')


if __name__ == '__main__':
    iterate_env()

from __future__ import print_function

import commands
import datetime
import json
import logging
import smtplib
import sys
import os
import time
import functools
from ConfigParser import SafeConfigParser
from email.mime.text import MIMEText

import requests
from redis import Redis

"""
    This should run every 10 minutes, and should only write to the slack channel once every 24 hours
    at 6am, providing the API is up and functioning.  If there are currently issues preventing the
    API from responding appropriately, then the program should write to the slack channel every 10
    minutes.  This program should also send an email to {ALL DCU ENGINEERS}@godaddy.com when the API is
    determined to be not functional

    Logging occurs in the file: ~root/apibot.log

    If you pass in a single argument "debug", then the program will run with parameters which
    will work on Brett Berry's MAC

    Message should indicate whether external API failed at the gateway, at sso or at the endpoint (Redis)
    and if it is failing at any of those, then it should go and check the internal
    1. Ping the SSO URL
    2. Authenticate via PyAuth (change to SCRAPI-DOO apibot endpoint)
    3. When we curl the apibot, we need to process the return and provide a more helpful message

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

""" SET UP LOGGING
"""

logging.basicConfig(filename='apibot.log', level=logging.INFO,
                    format="[%(levelname)s:%(asctime)s:%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s"
                    )
logger = logging.getLogger(__name__)

""" CHECK FOR DEBUG FLAG
"""

DEBUG = False
if len(sys.argv) > 1:
    if sys.argv[1].lower() == 'debug':
        DEBUG = True
        logger.setLevel(logging.DEBUG)
        logger.debug("Debug is ON")

""" SET UP CONSTANT PERTAINING TO RECHECK AFTER ANY INITIAL FAILURE
"""

RECHECK_SSO_FAILURE = 10  # num of times to re-check sso after a failure
RECHECK_EXT_CURL = 10  # "                      " external url
RECHECK_INT_CURL = 10  # "                      " internal url
RECHECK_SSO_DELAY = 1  # sleep time in seconds for sso re-check
RECHECK_CURL_DELAY = 5  # sleep time in seconds for endpoint re-curl
FAILURE_THRESHOLD = 0.7  # only send alerts when failure rate is 70% or greater

configp = SafeConfigParser()
endpoint_data = {}


class Payload(object):
    # The Payload class holds the payload data, with methods allowing the user to modify certain
    # data member variables on the fly, print the payload to slack, and email the payload to all
    # DCU engineers
    channel = '#dcu_alerts'
    if DEBUG:
        channel = '#queue_bot_test'
    username = 'API BOT'
    thumbs_up = ':+1:'
    thumbs_down = ':-1:'
    icon = ''
    text = ''
    email_addresses = ['bxberry', 'abean', 'amarlar', 'chauser', 'pmcconnell', 'lhalvorson', 'ebenson', 'spetersen',
                       'ssanfratello']
    if DEBUG:
        email_addresses = ['bxberry', 'abean']

    def set_text(self, env, msg, up=True):
        self.icon = self.thumbs_down

        if up:
            self.icon = self.thumbs_up

        self.text = "<!channel> {msg} for {env} Abuse API".format(
            msg=msg,
            env=env
        )

    def get_text(self):
        return self.text

    def get_payload(self):
        d_payload = {'payload': json.dumps({
            'channel': self.channel,
            'username': self.username,
            'icon_emoji': self.icon,
            'text': self.text
        })
        }
        return d_payload

    ''' Prints the payload to a defined slack channel
    '''

    def print_to_slack(self):
        resp = requests.post(configp.get('slack', 'slack_url'),
                             data=self.get_payload())
        logger.debug("Print to Slack response: " + str(resp))

    def email_payload(self):
        msg = MIMEText(self.get_text())
        msg['Subject'] = self.text
        from_addr = 'DoNotReply@ApiBot.godaddy.com'
        msg['From'] = from_addr

        try:
            s = smtplib.SMTP('localhost')
            for address in enumerate(self.email_addresses):
                to = '{addr}@godaddy.com'.format(addr=address[1])
                msg['To'] = to
                s.sendmail(from_addr, to, msg.as_string())
            s.quit()

        except Exception as e:
            logger.error("Error sending mail: " + str(e))

    def report_message(self, message):
        logger.error(message)
        self.print_to_slack()
        self.email_payload()


""" SET UP OUR PAYLOAD OBJECT
"""

payload = Payload()

""" Set the error flag in Redis and alert via Slack & email
"""


def procedural_set_error(myenv, redis, error_flag, message):
    try:
        curval = redis.get(error_flag)
        if curval is None:
            payload.set_text(myenv, message, False)
            payload.report_message(message)

            ''' Set Redis error flag key
            '''
            redis.set(error_flag, "1")
    except Exception as e:
        logger.error(e.message)


""" Clear the error flag in Redis and alert via Slack
"""


def procedural_recover_error(myenv, redis, error_flag):

    """ Check Redis for error flag key """
    error_flag_val = redis.get(error_flag)

    ''' if error flag key, print '''
    if error_flag_val == "1":
        ''' SLACK the fact that the API recovered
        '''
        message = "API RECOVERED"
        payload.set_text(myenv, message)
        payload.print_to_slack()

        ''' Clear Redis error flag key '''
        redis.delete(error_flag)

    else:

        ''' If No Redis error flag key, check current time
            print if current time is between 6:00am & 6:09am
        '''

        six = datetime.datetime.strptime('06:00', '%H:%M')
        sixnine = datetime.datetime.strptime('06:09', '%H:%M')
        curtime = datetime.datetime.now()

        if six.time() <= curtime.time() <= sixnine.time():
            message = "API OK"
            payload.set_text(myenv, message)
            payload.print_to_slack()

    ''' Clear Redis error flag key
    '''
    redis.delete(error_flag)


def procedural_check_sso(myenv, session):
    if DEBUG:
        print("In procedural_check_sso(): " + str(myenv))
    logger.debug("In procedural_check_sso(): " + str(myenv))
    # Stop bot from breaking on ote check
    if myenv == 'ote':
        return True

    ''' Check to see if the SSO is accessible
    '''

    try:

        ''' Stagger the re-checks by RECHECK_TIME_DELAY seconds
        '''
        time.sleep(RECHECK_SSO_DELAY)
        status = session.get(endpoint_data[myenv]['sso_url'])

        if status.status_code == 200:
            return True
        else:
            return False

    except Exception as e:
        logger.error("Error running {}:{}".format(endpoint_data[myenv]['sso_url'], str(e)))
        return False


def procedural_curl_endpoint(myenv, curl_string):
    try:

        if DEBUG:
            print("In procedural_curl_endpoint(): " + str(myenv))
        logger.debug("In procedural_curl_endpoint(): " + str(myenv))

        ''' Stagger the re-checks by RECHECK_TIME_DELAY seconds
        '''
        time.sleep(RECHECK_CURL_DELAY)

        status, output = commands.getstatusoutput(curl_string)
        curl_status = "Status:{status}, Output:{output}".format(status=status, output=output)
        logger.info(curl_status)

    except Exception as e:
        logger.error("ERROR: " + myenv + ": Unknown Error running '" + str(curl_string) + "': " + str(e))
        return False

    ''' CURLing endpoint was successful
    '''
    if output[-2:] == 'ok':
        return True
    return False


def procedural_check(myenv, redis, error_flag):

    # Check the SSO URL to see if it is responsive
    with requests.sessions.Session() as session:
        if not procedural_check_sso(myenv[0], session):
            result_set = map(functools.partial(procedural_check_sso, session=session),
                             myenv * RECHECK_SSO_FAILURE)
            if (result_set.count(False)/RECHECK_SSO_FAILURE) >= FAILURE_THRESHOLD:
                message = "WARNING: {env} SSO Failed in {a} out of {b} attempts - Issue: {reason}:{url}".format(
                    env=myenv[0],
                    a=result_set.count(False),
                    b=RECHECK_SSO_FAILURE,
                    reason="Unable to communicate with SSO URL",
                    url=endpoint_data[myenv[0]]['sso_url']
                )

                payload.set_text(myenv, message, False)
                payload.report_message(message)

    # Curl the External API endpoint

    curl_string = "curl -XGET -H 'Content-Type: application/json' -H " \
                  "'Authorization: sso-key {key_secret}' {url} ".format(
                        key_secret=endpoint_data[myenv[0]]['key_secret'],
                        url=endpoint_data[myenv[0]]['ext_url']
                    )
    logger.info("CURLing: " + curl_string)

    if procedural_curl_endpoint(myenv[0], curl_string):

        # Success curling external url endpoint, so recover if a previous failure exists
        procedural_recover_error(myenv, redis, error_flag)

    else:

        # Failure curling external endpoint, so re-run the curl X number of times using map
        result_set = map(procedural_curl_endpoint,
                         myenv * RECHECK_EXT_CURL,
                         [curl_string] * RECHECK_EXT_CURL)

        if (result_set.count(False)/RECHECK_EXT_CURL) >= FAILURE_THRESHOLD:

            # External endpoint is degraded, so curl internal endpoint

            message = "FATAL: API DOWN for {env} Abuse API - {a} out of {b} attempts failed - Reason: {reason}".format(
                env=myenv[0],
                reason="External Endpoint Not Responding / Check PLATAPI and LBASS",
                a=result_set.count(False),
                b=RECHECK_EXT_CURL
            )

            curl_string = "curl -XGET -H 'Content-Type: application/json' -H " \
                          "'Authorization: sso-key {key_secret}' {url} ".format(
                                key_secret=endpoint_data[myenv[0]]['key_secret'],
                                url=endpoint_data[myenv[0]]['int_url']
                            )
            logger.info("CURLing: " + curl_string)

            if not procedural_curl_endpoint(myenv[0], curl_string):
                ''' External endpoint failed.  Re-run X number of times using map '''
                result_set = map(procedural_curl_endpoint,
                                 myenv * RECHECK_INT_CURL,
                                 [curl_string] * RECHECK_INT_CURL)

                if (result_set.count(False)/RECHECK_INT_CURL) >= FAILURE_THRESHOLD:
                    message = "FATAL: API DOWN for {env} Abuse API -  {a} out of {b} attempts failed - Reason: {reason}".format(
                        env=myenv[0],
                        reason="Internal Endpoint Not Responding / Check RANCHER and REDIS",
                        a=result_set.count(False),
                        b=RECHECK_INT_CURL
                    )

            # Alert the endpoint response degradation

            procedural_set_error(myenv, redis, error_flag, message)

        else:
            procedural_recover_error(myenv, redis, error_flag)


""" A method to cycle through the endpoint_data dictionary and process
    each environment separately.
"""


def iterate_env():
    """ We use redis to archive the error flag,
        so we know if we've recovered from an issue
    """
    logger.info("Starting...")
    global endpoint_data

    try:
        """
        Read configuration file
        """
        dir_path = os.path.dirname(os.path.realpath(__file__))
        configp.read('{}/api_bot_settings.ini'.format(dir_path))

        """ This is the environment specific data
        """

        endpoint_data = {
            'ote': dict(configp.items('ote')),
            'prod': dict(configp.items('prod')),
        }

        if DEBUG:
            endpoint_data['dev'] = dict(configp.items('dev'))
    except Exception as e:
        logger.fatal('Configuration error: {}'.format(e.message))

    try:
        redis = Redis(host='localhost', port=6379, password=endpoint_data.get('prod', {}).get('redis_password'))
    except Exception as e:
        logger.error("FATAL: Cant connect to Redis: " + str(e))
        return

    for key, envdict in endpoint_data.iteritems():
        '''If need to turn off OTE bot, uncomment the if key ==ote code/continue block below'''
        # if key == "ote":
        #     continue
        if time.strftime("%a").startswith('S') and key != "prod":
            ''' DO NOT RUN ANYTHING BUT PROD ON WEEKENDS '''
            continue
        logger.debug("Checking " + key + " environment")
        error_flag = "error_{key}".format(key=key)
        procedural_check([key], redis, error_flag)

    logger.info("Finishing...")


if __name__ == '__main__':
    iterate_env()

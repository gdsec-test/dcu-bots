import logging
from configparser import SafeConfigParser
import json
import requests
import pymongo
import os

logging.basicConfig(
    filename='slabot.log',
    level=logging.INFO,
    format="[%(levelname)s:%(asctime)s:%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s"
)
logger = logging.getLogger(__name__)

configp = SafeConfigParser()
dir_path = os.path.dirname(os.path.realpath(__file__))
configp.read('{}/sla_bot_settings.ini'.format(dir_path))


class Payload(object):
    # The Payload class holds the payload data, with methods allowing the user to modify certain
    # data member variables on the fly, print the payload to slack, and email the payload to all
    # DCU engineers

    def __init__(self):
        self.channel = '#dcu_sla'
        self.username = 'SLA BOT'
        self.icon = ''
        self.text = ''
        self._logger = logging.getLogger(__name__)

    def set_text(self, msg):
        self.text = "<!channel> API SLA {msg}".format(msg=msg)

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
        self._logger.debug("Print to Slack response: " + str(resp))

    def report_message(self, message):
        self._logger.error(message)
        self.print_to_slack()


class APISla(object):

    def __init__(self):
        self._logger = logging.getLogger(__name__)
        self._conn = pymongo.MongoClient(configp.get('db', 'db_url'), connect=False)
        self._db = self._conn[configp.get('db', 'api_db')]
        self._collection = self._db[configp.get('db', 'collection')]

    def get_sla(self):
        try:
            query = [
                {'$project': {'time': {'$divide': [{'$subtract': ['$closed', '$created']}, 60 * 60 * 1000]}}},
                {'$group': {'_id': None, 'sla': {'$avg': '$time'}}}
            ]
            data = list(self._collection.aggregate(query))
            return data[0].get('sla')
        except Exception as e:
            self._logger.error("Error calculating SLA:{}".format(e))


if __name__ == '__main__':
    logger.info("Starting API SLA calculation")
    sla = APISla()
    hrs = int(sla.get_sla())
    payload = Payload()
    payload.set_text("{} hours".format(hrs))
    payload.print_to_slack()
    logger.info("Finished API SLA calculation")


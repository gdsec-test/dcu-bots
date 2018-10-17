import logging
import pymongo
import pika
import json
import os
import yaml
from logging.config import dictConfig
from datetime import datetime, timedelta


def time_format(dt):
    """
    Function takes in a datetime object and returns a YYYY-mm-ddTHH:MM:SS.fffZ formatted string
    :param dt: datetime object
    :return: string
    """
    if type(dt) not in [datetime]:
        logger.error('Received unexpected type: {}'.format(type(dt)))
        return dt
    return "%s:%.3f%sZ" % (dt.strftime('%Y-%m-%dT%H:%M'),
                           float("%.3f" % (dt.second + dt.microsecond / 1e6)),
                           dt.strftime('%z'))


class MongoHelperAPI:
    def __init__(self):
        self._logger = logging.getLogger(__name__)
        _dbuser = os.getenv('DB_USER') or 'user'
        _dbpass = os.getenv('DB_PASS') or 'password'
        self._conn = pymongo.MongoClient('mongodb://{}:{}@10.47.4.165/dev'.format(_dbuser, _dbpass))
        self._db = self._conn['dev']
        self._collection = self._db['incidents']

    def handle(self):
        return self._collection


# class Publisher:
#     EXCHANGE = 'ticket-stats'
#     TYPE = 'direct'
#     ROUTING_KEY = 'ticket-stats'
#
#     def __init__(self, host, port, virtual_host, username, password):
#         self._params = pika.connection.ConnectionParameters(
#             host=host,
#             port=port,
#             virtual_host=virtual_host,
#             credentials=pika.credentials.PlainCredentials(username, password),
#             ssl=True)
#         self._conn = None
#         self._channel = None
#         self._logger = logging.getLogger(__name__)
#
#     def connect(self):
#         if not self._conn or self._conn.is_closed:
#             self._conn = pika.BlockingConnection(self._params)
#             self._channel = self._conn.channel()
#             self._channel.exchange_declare(
#                 exchange=self.EXCHANGE, exchange_type=self.TYPE, durable=True)
#
#     def _publish(self, msg):
#         self._channel.basic_publish(
#             exchange=self.EXCHANGE,
#             routing_key=self.ROUTING_KEY,
#             body=json.dumps(msg).encode())
#         self._logger.debug('message sent: %s', msg)
#
#     def publish(self, msg):
#         """Publish msg, reconnecting if necessary."""
#
#         try:
#             self._publish(msg)
#         except pika.exceptions.ConnectionClosed:
#             self._logger.debug('reconnecting to queue')
#             self.connect()
#             self._publish(msg)
#
#     def close(self):
#         if self._conn and self._conn.is_open:
#             self._logger.debug('closing queue connection')
#             self._conn.close()
#
#
# def merge_dicts(a, b):
#     z = a.copy()
#     z.update(b)
#     return z


if __name__ == '__main__':

    # path = ''
    # value = os.getenv('LOG_CFG', None)
    # if value:
    #     path = value
    # if os.path.exists(path):
    #     with open(path, 'rt') as f:
    #         lconfig = yaml.safe_load(f.read())
    #     dictConfig(lconfig)
    # else:
    #     logging.basicConfig(level=logging.INFO)
    # logger = logging.getLogger(__name__)

    # logger.info("Starting Kelvin stats retrieval")
    mongo = MongoHelperAPI()
    # rabbit = Publisher(
    #     host='rmq-dcu.int.godaddy.com',
    #     port=5672,
    #     virtual_host='grandma',
    #     username=os.getenv('BROKER_USER') or 'user',
    #     password=os.getenv('BROKER_PASS') or 'password')
    # rabbit.connect()

    #  ToDo: Do we need to adjust this lastModified timedelta to something other than 1 hr for initial run to go back
    #  and grab all the data ?
    for data in mongo.handle().find({'$or': [{'lastModified': {'$gte': datetime.utcnow() - timedelta(hours=1)}},
                                             {'closedAt': {'$gte': datetime.utcnow() - timedelta(hours=1)}}]}):
        data.pop('_id', None)
        data.pop('source', None)
        data.pop('ticketID', None)
        data.pop('ticketCategory', None)
        data.pop('type', None)
        data.pop('target', None)
        data.pop('archiveCompleted', None)
        data.pop('reporterEmail', None)
        data.pop('lastModified', None)

    #     data.pop('hold_until', None)
    #     data.pop('last_modified', None)

        cmap_data = data.pop('data', None)
        if cmap_data:
            host_data = cmap_data.get('domainQuery', {}).get('host')
            if host_data:
                host_brand = host_data.get('brand')
                host_product = host_data.get('product')
                host_data_center = host_data.get('dataCenter')
                host_ip = host_data.get('ip')
                hostname = host_data.get('hostname')
                hosting_company_name = host_data.get('hostingCompanyName')
                host_create_date = host_data.get('createDate')
                mwp_id = host_data.get('mwpId')
                host_guid = host_data.get('guid')
                host_os = host_data.get('os')

                if host_brand:
                    data['host_brand'] = host_brand
                if host_product:
                    data['host_product'] = host_product
                if host_data_center:
                    data['host_data_center'] = host_data_center
                if host_ip:
                    data['host_ip'] = host_ip
                if hostname:
                    data['hostname'] = hostname
                if hosting_company_name:
                    data['hosting_company_name'] = hosting_company_name
                if host_create_date:
                    data['host_create_date'] = host_create_date
                if mwp_id:
                    data['mwp_id'] = mwp_id
                if host_guid:
                    data['host_guid'] = host_guid
                if host_os:
                    data['host_os'] = host_os

            shopper_data = cmap_data.get('domainQuery', {}).get('shopperInfo')

            registrar_data = cmap_data.get('domainQuery', {}).get('registrar')


        print data
    #     meta = data.pop('metadata', None)
    #     if meta:
    #         merge_dicts(data, meta)
    #     for time in ['created', 'closed', 'iris_created']:
    #         tdata = data.get(time)
    #         if tdata:
    #             data[time] = time_format(tdata)
    #         else:
    #             if time == 'created':
    #                 opent = data.get('closed')
    #                 data[time] = time_format(opent)
    #     rabbit.publish(data)
    # logger.info("Finished ticket stats retrieval")

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
        _dbuser = os.getenv('DB_USER', 'user')
        _dbpass = os.getenv('DB_PASS', 'password')
        self._conn = pymongo.MongoClient('mongodb://{}:{}@10.22.9.209/dcu_kelvin'.format(_dbuser, _dbpass))
        self._db = self._conn['dcu_kelvin']
        self._collection = self._db['incidents']

    def handle(self):
        return self._collection


class Publisher:
    EXCHANGE = 'kelvin-stats'
    TYPE = 'direct'
    ROUTING_KEY = 'kelvin-stats'

    def __init__(self, host, port, virtual_host, username, password):
        self._params = pika.connection.ConnectionParameters(
            host=host,
            port=port,
            virtual_host=virtual_host,
            credentials=pika.credentials.PlainCredentials(username, password),
            ssl=True)
        self._conn = None
        self._channel = None
        self._logger = logging.getLogger(__name__)

    def connect(self):
        if not self._conn or self._conn.is_closed:
            self._conn = pika.BlockingConnection(self._params)
            self._channel = self._conn.channel()
            self._channel.exchange_declare(
                exchange=self.EXCHANGE, exchange_type=self.TYPE, durable=True)

    def _publish(self, msg):
        self._channel.basic_publish(
            exchange=self.EXCHANGE,
            routing_key=self.ROUTING_KEY,
            body=json.dumps(msg).encode())
        self._logger.debug('message sent: %s', msg)

    def publish(self, msg):
        """Publish msg, reconnecting if necessary."""

        try:
            self._publish(msg)
        except pika.exceptions.ConnectionClosed:
            self._logger.debug('reconnecting to queue')
            self.connect()
            self._publish(msg)

    def close(self):
        if self._conn and self._conn.is_open:
            self._logger.debug('closing queue connection')
            self._conn.close()


def merge_dicts(a, b):
    z = a.copy()
    z.update(b)
    return z


if __name__ == '__main__':

    path = ''
    value = os.getenv('LOG_CFG', None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            lconfig = yaml.safe_load(f.read())
        dictConfig(lconfig)
    else:
        logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    logger.info("Starting Kelvin stats retrieval")
    mongo = MongoHelperAPI()
    rabbit = Publisher(
        host='rmq-dcu.int.godaddy.com',
        port=5672,
        virtual_host='grandma',
        username=os.getenv('BROKER_USER', 'user'),
        password=os.getenv('BROKER_PASS', 'password'))
    rabbit.connect()

    for data in mongo.handle().find({'$or': [{'lastModified': {'$gte': datetime.utcnow() - timedelta(hours=1)}},
                                             {'closedAt': {'$gte': datetime.utcnow() - timedelta(hours=1)}}]}):
        data.pop('_id', None)
        data.pop('ticketCategory', None)
        data.pop('source', None)
        data.pop('sourceDomainOrIP', None)
        data.pop('info', None)
        data.pop('target', None)
        data.pop('reporterEmail', None)
        data.pop('lastModified', None)
        data.pop('archiveCompleted', None)
        data.pop('fileInfo', None)
        data.pop('reportFileID', None)
        data.pop('ncmecReportID', None)
        data.pop('userGenHoldReason', None)
        data.pop('userGenHoldUntil', None)
        data.pop('messageID', None)
        data.pop('notified', None)

        domain_brand_data = data.pop('domain', None)
        if domain_brand_data:
            domain_brand = domain_brand_data.get('brand')
            domain_registrar = domain_brand_data.get('registrarName')
            domain_create_date = domain_brand_data.get('domainCreateDate')

            if domain_brand:
                data['domain_brand'] = domain_brand
            if domain_registrar:
                data['domain_registrar'] = domain_registrar
            if domain_create_date:
                if 'T' not in domain_create_date:
                    domain_create_date += 'T00:00:00Z'
                data['domain_create_date'] = domain_create_date

        hosting_brand_data = data.pop('hosting', None)
        if hosting_brand_data:
            hosting_brand = hosting_brand_data.get('brand')
            hosting_company_name = hosting_brand_data.get('hostingCompanyName')
            hosting_ip = hosting_brand_data.get('IP')

            if hosting_brand:
                data['hosting_brand'] = hosting_brand
            if hosting_company_name:
                data['hosting_company_name'] = hosting_company_name
            if hosting_ip:
                data['hosting_ip'] = hosting_ip

        cmap_data = data.pop('data', None)
        if cmap_data:
            host_data = cmap_data.get('domainQuery', {}).get('host')

            if host_data:
                host_product = host_data.get('product')
                host_data_center = host_data.get('dataCenter')
                hostname = host_data.get('hostname')
                mwp_id = host_data.get('mwpId')
                host_guid = host_data.get('guid')
                host_os = host_data.get('os')

                if host_product:
                    data['host_product'] = host_product
                if host_data_center:
                    data['host_data_center'] = host_data_center
                if hostname:
                    data['hostname'] = hostname
                if mwp_id:
                    data['mwp_id'] = mwp_id
                if host_guid:
                    data['host_guid'] = host_guid
                if host_os:
                    data['host_os'] = host_os

            shopper_data = cmap_data.get('domainQuery', {}).get('shopperInfo')
            if shopper_data:
                shopper_city = shopper_data.get('shopperCity')
                shopper_state = shopper_data.get('shopperState')
                shopper_country = shopper_data.get('shopperCountry')

                if shopper_city:
                    data['shopper_city'] = shopper_city
                if shopper_state:
                    data['shopper_state'] = shopper_state
                if shopper_country:
                    data['shopper_country'] = shopper_country

        meta = data.pop('metadata', None)

        if meta:
            meta.pop('iris_id')
            data = merge_dicts(data, meta)
        for time in ['createdAt', 'closedAt', 'iris_created']:
            tdata = data.get(time)
            if tdata:
                data[time] = time_format(tdata)
        rabbit.publish(data)
    logger.info("Finished Kelvin stats retrieval")

import json
import logging
import os
import re
import sys
import tld
from configparser import ConfigParser
from datetime import datetime
from dateutil import parser
from logging.config import dictConfig
from tld.conf import set_setting
from urllib.parse import quote

import requests
import yaml
from celery import Celery
from kombu import Exchange, Queue
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from requests import sessions


class CeleryConfig:
    BROKER_TRANSPORT = 'pyamqp'
    BROKER_USE_SSL = True
    CELERY_TASK_SERIALIZER = 'pickle'
    CELERY_RESULT_SERIALIZER = 'json'
    CELERY_ACCEPT_CONTENT = ['json']
    CELERY_IMPORTS = 'run'
    CELERYD_HIJACK_ROOT_LOGGER = False

    def __init__(self, _settings: dict):
        """
        :param _settings: dict
        """
        _queue = _settings.get('celery_queue')
        _task = _settings.get('celery_task')

        self.CELERY_QUEUES = (
            Queue(_queue, Exchange(_queue), routing_key=_queue),
        )
        self.CELERY_ROUTES = {_task: {'queue': _queue}}
        self.BROKER_URL = 'amqp://{}:{}@{}'.format(_settings.get('broker_user'),
                                                   quote(_settings.get('broker_pass')),
                                                   _settings.get('broker_url'))


class CMAPHelper:
    """
    Helper class to query CMAP Service
    """
    DATE_FORMAT = '%Y-%m-%d'
    HEADERS = {'Content-Type': 'application/graphql'}
    KEY_DCD = 'domainCreateDate'
    KEY_SCD = 'shopperCreateDate'
    KEY_REG = 'registrar'
    KEY_SI = 'shopperInfo'

    def __init__(self, _env_settings: dict):
        """
        :param _env_settings: dict
        """
        self._logger = logging.getLogger(__name__)
        self._url = _env_settings.get('cmap_service_url')
        self._sso_endpoint = f'{_env_settings.get("sso_url")}/v1/secure/api/token'
        _cert = (_env_settings.get('cmap_service_cert'), _env_settings.get('cmap_service_key'))
        self.HEADERS.update({'Authorization': f'sso-jwt {self._get_jwt(_cert)}'})

    def _convert_unicode_to_date(self, _data_dict: dict) -> dict:
        """
        Registrar.domainCreateDate and shopperInfo.shopperCreateDate are returned in unicode, but we want to
        insert as date
        :param _data_dict: dict of cmap results
        :return: dict
        """
        if not isinstance(_data_dict, dict):
            return {}
        _dq = _data_dict.get('data', {}).get('domainQuery', {})
        if not _dq:
            return {}
        # Convert domainCreateDate if exists
        if _dq.get(self.KEY_REG, {}).get(self.KEY_DCD):
            _dq[self.KEY_REG][self.KEY_DCD] = parser.parse(_dq.get(self.KEY_REG, {}).get(self.KEY_DCD))
        # Convert shopperCreateDate if exists
        if _dq.get(self.KEY_SI, {}).get(self.KEY_SCD):
            _dq[self.KEY_SI][self.KEY_SCD] = parser.parse(_dq.get(self.KEY_SI, {}).get(self.KEY_SCD))
        return _data_dict

    def cmap_query(self, _domain: str) -> dict:
        """
        Returns query result of cmap service given a domain
        :param _domain:
        :return: dict
        """
        with sessions.Session() as _session:
            _re = _session.post(url=self._url, headers=self.HEADERS, data=CMAPHelper._get_query(_domain))
            _data = self._convert_unicode_to_date(json.loads(_re.text))
            return _data

    @staticmethod
    def determine_hosted_status(_host_brand: str, _registrar_brand: str) -> str:
        """
        Will return the determined hosted status based on the brands provided
        :param _host_brand: string representing the brand which the domain is hosted with
        :param _registrar_brand: string representing the brand which the domain is registered with
        :return: string of determined hosted status
        """
        _hosted_status = 'UNKNOWN'
        if not _host_brand:
            _host_brand = 'UNKNOWN'
        if not _registrar_brand:
            _registrar_brand = 'UNKNOWN'

        if _host_brand.upper() == 'GODADDY':
            _hosted_status = 'HOSTED'
        elif _registrar_brand.upper() == 'GODADDY':
            _hosted_status = 'REGISTERED'
        elif _host_brand.upper() == 'FOREIGN':
            _hosted_status = 'FOREIGN'
        return _hosted_status

    @staticmethod
    def _get_query(_domain_to_query: str) -> str:
        """
        This returns the query that Kelvin Service uses
        :param _domain_to_query: string domain name to query
        :return: string of GraphQL query, including domain name to query
        """
        return '''
{
    domainQuery(domain: "''' + _domain_to_query + '''") {
        domain
        host {
            brand
            guid
            hostingAbuseEmail
            hostingCompanyName
            ip
            product
            shopperId
        }
        registrar {
            brand
            domainCreateDate
            domainId
            registrarAbuseEmail
            registrarName
        }
        shopperInfo {
            shopperCreateDate
            shopperId
        }
    }
}
'''

    def _get_jwt(self, _cert: tuple) -> str:
        """
        Attempt to retrieve the JWT associated with the cert/key pair from SSO
        body data should resemble: {'type': 'signed-jwt', 'id': 'XXX', 'code': 1, 'message': 'Success', 'data': JWT}
        :param _cert:
        :return: jwt
        """
        _response = requests.post(self._sso_endpoint, data={'realm': 'cert'}, cert=_cert)
        _response.raise_for_status()

        _body = json.loads(_response.text)
        return _body.get('data')


class DBHelper:
    """
    DB helper class specific to the Kelvin databases
    """
    KEY_KELVIN_STATUS = 'kelvinStatus'
    KEY_USERGEN = 'userGen'

    def __init__(self, _env_settings: dict, _cmap_service: CMAPHelper, _db_name: str, _db_user: str,
                 _db_pass: str, _kelvin: bool = False):
        """
        :param _env_settings: dict from ini settings file
        :param _cmap_service: handle to the cmap service helper
        :param _db_name: string
        :param _db_user: string
        :param _db_pass: string
        :param _kelvin: bool if Kelvin tickets
        """
        self._logger = logging.getLogger(__name__)
        _client = MongoClient(_env_settings.get('db_url'))
        _client[_env_settings.get(_db_name)].authenticate(_env_settings.get(_db_user),
                                                          _env_settings.get(_db_pass),
                                                          mechanism=_env_settings.get('db_auth_mechanism'))
        _db = _client[_settings.get(_db_name)]
        self._collection = _db.incidents
        self._pdna_reporter = _env_settings.get('pdna_reporter_id')
        self._cmap = _cmap_service
        self._client = _client
        self._kelvin = _kelvin

        _capp = Celery()
        _capp.config_from_object(CeleryConfig(_env_settings))
        self._celery = _capp

    def close_connection(self) -> None:
        """
        Closes the connection to the db
        :return: None
        """
        self._client.close()

    def scrub_domain_ip_from_url(self, _source: str) -> str:
        """
        Need a way to scrub the domain/ip from the URL
        :param _source:
        :return:
        """
        set_setting(
            'NAMES_LOCAL_PATH',
            '/tmp/names.dat'
        )
        tld.utils.PROJECT_DIR = lambda x: x
        hold_domain = _source
        subdomain = ''

        hold_domain = re.sub('https?://', '', hold_domain)  # Remove any http:// or https://

        is_ip = re.match('^((\\d{1,3}\\.){3}(\\d{1,3}))', hold_domain)
        if is_ip:
            pass
        else:  # Check for ipv6 match
            is_ip = re.match('\\[?(([a-fA-F0-9]{0,4}:){1,7}[a-fA-F0-9]{0,4})', hold_domain)
            if not is_ip:
                try:
                    domain_object = tld.get_tld(_source, as_object=True)
                    domain = domain_object.fld
                    if domain_object.subdomain is None or len(domain_object.subdomain) == 0:
                        subdomain = domain
                    else:
                        subdomain = f'{domain_object.subdomain}.{domain}'

                except Exception:
                    self._logger.warning(f'{_source} not found in tld file...updating and retrying')
                    tld.update_tld_names()

                    tld.utils.tld_names = {}  # Clears out global tld_names to force it to update

        return subdomain  # Prevents None being passed to gRPC calls expecting strings

    def _convert_snow_ticket_to_mongo_record_kelvin(self, _snow_ticket: dict) -> dict:
        """
        Builds a dict in the format of a db record
        :param _snow_ticket: dict of SNOW ticket key/value pairs
        :return: dict of DB record key/value pairs
        """
        _db_record = {
            'createdAt': datetime.strptime(_snow_ticket.get('sys_created_on'), '%Y-%m-%d %H:%M:%S'),
            self.KEY_KELVIN_STATUS: 'OPEN' if _snow_ticket.get('u_is_ticket_closed') else 'CLOSED',
            'ticketID': _snow_ticket.get('u_number'),
            'source': _snow_ticket.get('u_source'),
            'sourceDomainOrIP': _snow_ticket.get('u_source_domain_or_ip'),
            'type': _snow_ticket.get('u_type'),
            'target': _snow_ticket.get('u_target', ''),
            'proxy': _snow_ticket.get('u_proxy_ip', ''),
            'reporter': _snow_ticket.get('u_reporter')
        }
        if _snow_ticket.get('u_info'):
            _db_record['info'] = _snow_ticket['u_info']
        if _snow_ticket.get(self.KEY_USERGEN):
            _db_record[self.KEY_USERGEN] = _snow_ticket[self.KEY_USERGEN]
        if _db_record['reporter'] == self._pdna_reporter and _db_record[self.KEY_KELVIN_STATUS] == 'OPEN':
            _db_record[self.KEY_KELVIN_STATUS] = 'AWAITING_INVESTIGATION'

        # Enrich db record
        _db_record.update(self._cmap.cmap_query(_snow_ticket.get('u_source_domain_or_ip')))
        dq = _db_record.get('data', {}).get('domainQuery', {})
        _db_record['hostedStatus'] = CMAPHelper.determine_hosted_status(dq.get('host', {}).get('brand'),
                                                                        dq.get('registrar', {}).get('brand'))
        return _db_record

    def _convert_snow_ticket_to_mongo_record_phishstory(self, _snow_ticket: dict) -> dict:
        """
        Builds a dict in the format of a db record
        :param _snow_ticket: dict of SNOW ticket key/value pairs
        :return: dict of DB record key/value pairs
        """
        subdomain = self.scrub_domain_ip_from_url(_snow_ticket.get('u_source'))
        _db_record = {
            '_id': _snow_ticket.get('u_number'),
            'created': datetime.strptime(_snow_ticket.get('sys_created_on'), '%Y-%m-%d %H:%M:%S'),
            'ticketId': _snow_ticket.get('u_number'),
            'source': _snow_ticket.get('u_source'),
            'sourceDomainOrIp': _snow_ticket.get('u_source_domain_or_ip'),
            'sourceSubDomain': subdomain,
            'type': _snow_ticket.get('u_type'),
            'target': _snow_ticket.get('u_target', ''),
            'proxy': _snow_ticket.get('u_proxy_ip', ''),
            'reporter': _snow_ticket.get('u_reporter')
        }
        if _snow_ticket.get('u_info'):
            _db_record['info'] = _snow_ticket['u_info']
        return _db_record

    def create_tickets_based_on_snow(self, _list_of_snow_tickets: list) -> None:
        """
        Loop through all SNOW tickets and if they don't exist in the DB, then create them
        :param _list_of_snow_tickets: list of dicts containing SNOW ticket key/value pairs
        :return: None
        """
        self._logger.info('Start DB Ticket Query/Creation')
        for _ticket in _list_of_snow_tickets:
            if self._kelvin:
                # Check to see if ticket id exists in DB
                if not self._collection.find_one({'ticketID': _ticket.get('u_number')}):
                    self._logger.info(f'Creating DB ticket for: {_ticket.get("u_number")}')
                    try:
                        _payload = self._convert_snow_ticket_to_mongo_record_kelvin(_ticket)
                        self._collection.insert_one(_payload)
                    except Exception as e:
                        self._logger.error(e)
            else:
                # Check to see if ticket id exists in DB
                if not self._collection.find_one({'_id': _ticket.get('u_number')}):
                    self._logger.info(f'Creating DB ticket for: {_ticket.get("u_number")}')
                    try:
                        _payload = self._convert_snow_ticket_to_mongo_record_phishstory(_ticket)
                        self._collection.insert_one(_payload)
                        self._send_to_middleware(_payload)
                    except DuplicateKeyError:
                        pass
                    except Exception as e:
                        self._logger.error(e)
        self._logger.info('Finish DB Ticket Query/Creation')

    def _send_to_middleware(self, _payload: dict) -> None:
        """
        A helper function to send Celery tasks to the Middleware Queue with the provided payload
        :param _payload:
        :return:
        """
        try:
            self._logger.info(f'Sending payload to Middleware {_payload.get("ticketId")}.')
            self._celery.send_task('run.process', (_payload,))
        except Exception as _e:
            self._logger.error(f'Unable to send payload to Middleware {_payload.get("ticketId")} {_e}.')


class SNOWHelper:
    """
    Get all tickets that were created in SNOW Kelvin after MongoDB was down
    """
    HEADERS = {'Content-Type': 'application/json', 'Accept': 'application/json'}

    def __init__(self, _env_settings: dict, _snow_table_url: str, _query_time: str):
        """
        :param _env_settings: dict from ini settings file
        :param _snow_table_url: string
        :param _query_time: string representing date to start querying tickets from
        """
        self._logger = logging.getLogger(__name__)
        self._url = _env_settings.get(_snow_table_url).format(querytime=_query_time)
        self._auth = (_env_settings.get('snow_user'), _env_settings.get('snow_pass'))

    def get_tickets_created_during_downtime(self) -> list:
        """
        Query SNOW to get all info for tickets created since _query_time
        :return: list of dicts containing snow tickets
        """
        self._logger.info('Start SNOW Ticket Retrieval')
        _data = []
        try:
            _response = requests.get(self._url, auth=self._auth, headers=self.HEADERS)
            if _response.status_code == 200:
                _data = _response.json().get('result', [])
            else:
                self._logger.error('Unable to retrieve tickets from SNOW API {}: {}'.format(_response.status_code,
                                                                                            _response.json()))
        except Exception as _err:
            self._logger.error(f'Exception while retrieving tickets from SNOW API {_err}')
        finally:
            self._logger.info('Finish SNOW Ticket Retrieval')
            return _data


def read_config() -> dict:
    """
    Reads the configuration ini file for the env specific settings
    :return: dict of configuration settings for the env
    """
    _dir_path = os.path.dirname(os.path.realpath(__file__))
    _config_p = ConfigParser()
    _config_p.read(f'{_dir_path}/connection_settings.ini')
    return dict(_config_p.items(os.getenv('sysenv', 'dev')))


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
    """
    This script should be used after the DCU Mongo database has been brought back up from an outage, as tickets
    submitted to the Abuse API were written into SNOW.  This script will search all SNOW tickets given a datetime (which
    needs to be defined by person running this script in the variable QUERY_TIME within the SNOWHelper class)
    and then query the database to see if the ticket is present.  If the ticket is not present, then the script will
    insert the record.
    """
    PROCESS_NAME = 'Mongo Downtime Ticket Process'

    # TODO: Need to provide a date and time to search from in the following format: 'YYYY-MM-DD','hh:mm:ss'
    QUERY_TIME = "'YYYY-MM-DD','HH:MM:SS'"

    _logger = setup_logging()

    _db_client = _settings = _cmap_client = None

    try:
        _settings = read_config()

        # Create handle to CMAP Service API
        _cmap_client = CMAPHelper(_settings)

    except Exception as _e:
        _logger.fatal(f'Cannot continue: {_e}')
        sys.exit(-1)

    _run_products = {
        'kelvin': {'url': 'snow_kelvin_url', 'db': 'db_k|db_user_k|db_pass_k'},
        'phishstory': {'url': 'snow_url', 'db': 'db|db_user|db_pass'}
    }

    for _name in _run_products:
        _logger.info(f'Started {PROCESS_NAME} for {_name}')
        try:
            # Create handle to SNOW
            _snow_client = SNOWHelper(_env_settings=_settings,
                                      _snow_table_url=_run_products.get(_name).get('url'),
                                      _query_time=QUERY_TIME)

            # Create handle to the DB
            _db_creds = _run_products.get(_name).get('db', '').split('|')
            _db_client = DBHelper(_env_settings=_settings,
                                  _cmap_service=_cmap_client,
                                  _db_name=_db_creds[0],
                                  _db_user=_db_creds[1],
                                  _db_pass=_db_creds[2],
                                  _kelvin=_name == 'kelvin')

            # Retrieve Mongo Downtime tickets from SNOW API
            _snow_tickets = _snow_client.get_tickets_created_during_downtime()

            # Pass snow ticket list to DB helper, to create tickets that dont exist in DB
            _db_client.create_tickets_based_on_snow(_snow_tickets)

        except Exception as _e:
            _logger.error(_e)
        finally:
            if _db_client:
                _db_client.close_connection()
                _db_client = None
            _logger.info(f'Finished {PROCESS_NAME} for {_name}\n')

import json
import logging
import os
import yaml
import requests
from requests import sessions
from datetime import datetime
from logging.config import dictConfig
from pymongo import MongoClient
from ConfigParser import SafeConfigParser


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

    def __init__(self, env_settings):
        self._logger = logging.getLogger(__name__)
        self._url = env_settings.get('cmap_service_url')
        self._sso_endpoint = '{}/v1/secure/api/token'.format(env_settings.get('sso_url'))
        cert = (env_settings.get('cmap_service_cert'), env_settings.get('cmap_service_key'))
        self.HEADERS.update({'Authorization': 'sso-jwt {}'.format(self._get_jwt(cert))})

    def _convert_unicode_to_date(self, data_dict):
        """
        Registrar.domainCreateDate and shopperInfo.shopperCreateDate are returned in unicode, but we want to
        insert as date
        :param data_dict: dict of cmap results
        :return: dict
        """
        if not isinstance(data_dict, dict):
            return {}
        dq = data_dict.get('data', {}).get('domainQuery', {})
        if not dq:
            return {}
        # Convert domainCreateDate if exists
        if dq.get(self.KEY_REG, {}).get(self.KEY_DCD):
            dq[self.KEY_REG][self.KEY_DCD] = datetime.strptime(dq.get(self.KEY_REG, {}).get(self.KEY_DCD),
                                                               self.DATE_FORMAT)
        # Convert shopperCreateDate if exists
        if dq.get(self.KEY_SI, {}).get(self.KEY_SCD):
            dq[self.KEY_SI][self.KEY_SCD] = datetime.strptime(dq.get(self.KEY_SI, {}).get(self.KEY_SCD),
                                                              self.DATE_FORMAT)
        return data_dict

    def cmap_query(self, domain):
        """
        Returns query result of cmap service given a domain
        :param domain:
        :return: dict
        """
        with sessions.Session() as session:
            re = session.post(url=self._url, headers=self.HEADERS, data=CMAPHelper._get_query(domain))
            data = self._convert_unicode_to_date(json.loads(re.text))
            return data

    @staticmethod
    def determine_hosted_status(host_brand, registrar_brand):
        """
        Will return the determined hosted status based on the brands provided
        :param host_brand: string representing the brand which the domain is hosted with
        :param registrar_brand: string representing the brand which the domain is registered with
        :return: string of determined hosted status
        """
        hosted_status = 'UNKNOWN'
        if host_brand == 'GODADDY':
            hosted_status = 'HOSTED'
        elif registrar_brand == 'GODADDY':
            hosted_status = 'REGISTERED'
        elif host_brand == 'FOREIGN':
            hosted_status = 'FOREIGN'
        return hosted_status

    @staticmethod
    def _get_query(domain_to_query):
        """
        This returns the query that Kelvin Service uses
        :param domain_to_query: string domain name to query
        :return: dict of GraphQL query, including domain name to query
        """
        return '''
{
    domainQuery(domain: "''' + domain_to_query + '''") {
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

    def _get_jwt(self, cert):
        """
        Attempt to retrieve the JWT associated with the cert/key pair from SSO
        :param cert:
        :return: jwt
        """
        try:
            response = requests.post(self._sso_endpoint, data={'realm': 'cert'}, cert=cert)
            response.raise_for_status()

            body = json.loads(response.text)
            return body.get('data')  # {'type': 'signed-jwt', 'id': 'XXX', 'code': 1, 'message': 'Success', 'data': JWT}
        except Exception as err:
            self._logger.error(err.message)
        return None


class DBHelper:
    """
    DB helper class specific to the Kelvin databases
    """
    KEY_KELVIN_STATUS = 'kelvinStatus'
    KEY_USERGEN = 'userGen'

    def __init__(self, env_settings, cmap_service):
        """
        :param env_settings: dict from ini settings file
        :param cmap_service: handle to the cmap service helper
        """
        self._logger = logging.getLogger(__name__)
        client = MongoClient(env_settings.get('db_url'))
        client[env_settings.get('db_k')].authenticate(env_settings.get('db_user_k'),
                                                      env_settings.get('db_pass_k'),
                                                      mechanism=env_settings.get('db_auth_mechanism'))
        self._db = client[settings.get('db_k')]
        self._collection = self._db.incidents
        self._pdna_reporter = env_settings.get('pdna_reporter_id')
        self._cmap = cmap_service

    def close_connection(self):
        """
        Closes the connection to the db
        :return: None
        """
        self._db.close()

    def _convert_snow_ticket_to_mongo_record(self, snow_ticket):
        """
        Builds a dict in the format of a db record
        :param snow_ticket: dict of SNOW ticket key/value pairs
        :return: dict of DB record key/value pairs
        """
        db_record = {
            'createdAt': datetime.strptime(snow_ticket.get('sys_created_on'), '%Y-%m-%d %H:%M:%S'),
            self.KEY_KELVIN_STATUS: 'OPEN' if snow_ticket.get('u_is_ticket_closed') else 'CLOSED',
            'ticketID': snow_ticket.get('u_number'),
            'source': snow_ticket.get('u_source'),
            'sourceDomainOrIP': snow_ticket.get('u_source_domain_or_ip'),
            'type': snow_ticket.get('u_type'),
            'target': snow_ticket.get('u_target', ''),
            'proxy': snow_ticket.get('u_proxy_ip', ''),
            'reporter': snow_ticket.get('u_reporter')
        }
        if snow_ticket.get('u_info'):
            db_record['info'] = snow_ticket['u_info']
        if snow_ticket.get(self.KEY_USERGEN):
            db_record[self.KEY_USERGEN] = snow_ticket[self.KEY_USERGEN]
        if db_record['reporter'] == self._pdna_reporter and db_record[self.KEY_KELVIN_STATUS] == 'OPEN':
            db_record[self.KEY_KELVIN_STATUS] = 'AWAITING_INVESTIGATION'

        # Enrich db record
        db_record.update(self._cmap.cmap_query(snow_ticket.get('u_source_domain_or_ip')))
        dq = db_record.get('data', {}).get('domainQuery', {})
        db_record['hostedStatus'] = CMAPHelper.determine_hosted_status(dq.get('host', {}).get('brand'),
                                                                       dq.get('registrar', {}).get('brand'))
        return db_record

    def create_tickets_based_on_snow(self, list_of_snow_tickets):
        """
        Loop through all SNOW tickets and if they don't exist in the DB, then create them
        :param list_of_snow_tickets: list of dicts containing SNOW ticket key/value pairs
        :return: None
        """
        self._logger.info("Start DB Ticket Query/Creation")
        for ticket in list_of_snow_tickets:
            # Check to see if ticket id exists in DB
            if not self._collection.find_one({"ticketID": ticket.get('u_number')}):
                self._logger.info('Creating DB ticket for: {}'.format(ticket.get('u_number')))
                self._collection.insert_one(self._convert_snow_ticket_to_mongo_record(ticket))
        self._logger.info("Finish DB Ticket Query/Creation")


class SNOWHelper:
    """
    Get all tickets that were created in SNOW Kelvin after MongoDB was down
    """
    HEADERS = {"Content-Type": "application/json", "Accept": "application/json"}
    # TODO: Set the QUERY_TIME to a datetime prior to when the db was taken offline
    # Need to provide a date and time to search from in the following format: 'YYYY-MM-DD','hh:mm:ss'
    QUERY_TIME = "'2020-08-01','0:0:0'"

    def __init__(self, env_settings):
        """
        :param env_settings: dict from ini settings file
        """
        self._logger = logging.getLogger(__name__)
        self._url = env_settings.get('snow_kelvin_url').format(querytime=self.QUERY_TIME)
        self._auth = (env_settings.get('snow_user'), env_settings.get('snow_pass'))

    def get_tickets_created_during_downtime(self):
        """
        Query SNOW to get all info for tickets created since self.QUERY_TIME
        :return: list of dicts containing snow tickets
        """
        self._logger.info("Start SNOW Ticket Retrieval")
        data = []
        try:
            response = requests.get(self._url, auth=self._auth, headers=self.HEADERS)
            if response.status_code == 200:
                data = response.json().get('result', [])
            else:
                self._logger.error('Unable to retrieve tickets from SNOW API {}: {}'.format(response.status_code,
                                                                                            response.json()))
        except Exception as err:
            self._logger.error('Exception while retrieving tickets from SNOW API {}'.format(err.message))
        finally:
            self._logger.info("Finish SNOW Ticket Retrieval")
            return data


def read_config():
    """
    Reads the configuration ini file for the env specific settings
    :return: dict of configuration settings for the env
    """
    dir_path = os.path.dirname(os.path.realpath(__file__))
    config_p = SafeConfigParser()
    config_p.read('{}/connection_settings.ini'.format(dir_path))
    return dict(config_p.items(os.getenv('sysenv', 'dev')))


def setup_logging():
    """
    Sets up logging
    :return: handle to the logger
    """
    try:
        path = './logging.yaml'
        if path and os.path.exists(path):
            with open(path, 'rt') as f:
                lconfig = yaml.safe_load(f.read())
            dictConfig(lconfig)
        else:
            logging.basicConfig(level=logging.INFO)
    except Exception:
        logging.basicConfig(level=logging.INFO)
    finally:
        return logging.getLogger(__name__)


if __name__ == '__main__':
    """
    This script should be used after the DCU Mongo database has been brought back up from an outage, as tickets
    submitted to the Abuse API were written into SNOW.  This script will search all SNOW tickets given a datetime
    and then query the database to see if the ticket is present.  If the ticket is not present, then the script will
    insert the record.
    """
    PROCESS_NAME = 'Mongo Downtime Ticket Process'

    logger = setup_logging()
    logger.info('Started {} for Kelvin'.format(PROCESS_NAME))

    db_client = None
    try:
        settings = read_config()

        # Create handle to SNOW
        snow_client = SNOWHelper(settings)

        # Create handle to CMAP Service API
        cmap_client = CMAPHelper(settings)

        # Create handle to the DB
        db_client = DBHelper(settings, cmap_client)

        # Retrieve Kelvin Mongo Downtime tickets from SNOW API
        snow_tickets = snow_client.get_tickets_created_during_downtime()

        # Pass snow ticket list to DB helper, to create tickets that dont exist in DB
        db_client.create_tickets_based_on_snow(snow_tickets)

    except Exception as e:
        logger.fatal(e.message)
    finally:
        if db_client:
            db_client.close_connection()
        logger.info('Finished {} for Kelvin\n'.format(PROCESS_NAME))

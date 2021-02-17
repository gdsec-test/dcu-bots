import os
from configparser import ConfigParser

import requests
from pymongo import MongoClient


def open_ticket_in_mongo(_open_snow_ticket, _collection):
    """
    :param _open_snow_ticket:
    :param _collection:
    :return: boolean
    """
    _tix_num, _sys_id = _open_snow_ticket  # _open_snow_ticket is a tuple
    # Setting the phishstory_status field to OPEN and then removing the close_reason and closed fields altogether
    try:
        _collection.update_one({'_id': _tix_num}, {'$set': {'phishstory_status': 'OPEN'}}, upsert=False)
        _collection.update_one({'_id': _tix_num}, {'$unset': {'close_reason': 1, 'closed': 1}}, upsert=False)
        return True
    except Exception as _e:
        print('MONGO Exception: {}'.format(_e))
        return False


def open_snow_tickets(_open_snow_url, _ticket, _headers):
    """
    :param _open_snow_url:
    :param _ticket:
    :param _headers:
    :return: boolean
    """
    _tix_num, _sys_id = _ticket  # _ticket is a tuple
    _url = '{}/{}'.format(_open_snow_url, _sys_id)
    # DO NOT change the syntax of message below, as the function above
    _message = 'Opening ticket {}'.format(_tix_num)
    _my_data = '{"u_closed":"false", "u_closed_date":""}'
    try:
        _snow_open_response = requests.put(_url,
                                           auth=(_settings.get('snow_user'), _settings.get('snow_pass')),
                                           headers=_headers,
                                           data=_my_data)
        if _snow_open_response.status_code != 200:
            _message = 'Status:', _snow_open_response.status_code, \
                       'Headers:', _snow_open_response.headers, \
                       'Error Response:', _snow_open_response.json()
            print(_message)
            return False
        print(_message)
        return True
    except Exception as _e:
        print('SNOW Exception: {}'.format(_e))
        return False


def get_all_specified_snow_tickets(_snow_query_url, _specific_snow_ids, _headers):
    """
    :param _snow_query_url:
    :param _specific_snow_ids:
    :param _headers:
    :return: dict
    """
    _all_numbers_list = []
    for _snow_id in _specific_snow_ids:
        _all_numbers_list.append('u_number%3D{}'.format(_snow_id))

    _all_numbers_string = '^OR'.join(_all_numbers_list)
    _snow_query_string = '{}{}'.format(_snow_query_url, _all_numbers_string)
    # Do the HTTP request to SNOW getting all OPEN ticket records
    _response = requests.get(_snow_query_string,
                             auth=(_settings.get('snow_user'), _settings.get('snow_pass')),
                             headers=_headers)

    # Check for HTTP response codes from SNOW for other than 200
    if _response.status_code != 200:
        print('Status:', _response.status_code,
              'Headers:', _response.headers,
              'Error Response:', _response.json())
        exit()

    # Decode the SNOW JSON response into a dictionary and use the data
    return _response.json()


def make_snow_sysid_number_list(_snow_query_url, _specific_snow_ids, _headers):
    """
    :param _snow_query_url:
    :param _specific_snow_ids:
    :param _headers:
    :return: list to ticket tuples which look like
    [{
        u 'sys_id': u '01eb3e1437c76a00362896d543990e1a',
        u 'u_number': u 'DCU000024037'
    }, {
        u 'sys_id': u '034da51e2bc3a24054a41bc5a8da15b1',
        u 'u_number': u 'DCU000024766'
    }]
    """
    _snow_sys_id_number_list = []
    _snow_records = get_all_specified_snow_tickets(_snow_query_url, _specific_snow_ids, _headers)['result']
    for _snow_id in _snow_records:
        _snow_tuple = (_snow_id['u_number'], _snow_id['sys_id'])
        _snow_sys_id_number_list.append(_snow_tuple)
    return _snow_sys_id_number_list


def read_config(_env):
    """
    Reads the configuration ini file for the env specific settings
    :param _env: string representing run environment
    :return: dict of configuration settings for the env
    """
    _dir_path = os.path.dirname(os.path.realpath(__file__))
    _config_p = ConfigParser()
    _config_p.read('{}/missed_tickets_settings.ini'.format(_dir_path))
    return dict(_config_p.items(_env))


if __name__ == '__main__':
    _settings = read_config(os.getenv('sysenv', 'dev'))

    # creating pymongo settings
    _client = MongoClient(_settings.get('db_url'))

    # authenticating to client
    _client[_settings.get('db')].authenticate(_settings.get('db_user'),
                                              _settings.get('db_pass'),
                                              mechanism=_settings.get('db_auth_mechanism'))

    # getting the mongo db
    _db = _client[_settings.get('db')]

    # getting the collection
    _collection = _db.incidents

    # Set proper SNOW request headers
    _headers = {"Content-Type": "application/json", "Accept": "application/json"}

    # Read the list of SNOW ticket ids to open
    _filename = 'closed_ticket_ids.txt'

    # Structure of closed_snow_ticket_ids looks like:
    # ['DCU000025036', 'DCU000024953']
    _closed_snow_ticket_ids = [_line.strip() for _line in open(_filename)]

    # Structure of specific_closed_tickets looks like:
    # [(u 'DCU000024037', u '01eb3e1437c76a00362896d543990e1a'),
    #  (u 'DCU000024766', u '034da51e2bc3a24054a41bc5a8da15b1')]
    _specific_closed_tickets = make_snow_sysid_number_list(_settings.get('snow_url_query'),
                                                           _closed_snow_ticket_ids,
                                                           _headers)

    for _snow_ticket in _specific_closed_tickets:
        if not open_snow_tickets(_settings.get('snow_url_open'),
                                 _snow_ticket,
                                 _headers):
            print('ERROR OPENING SNOW TICKET: {}'.format(_snow_ticket))
        if not open_ticket_in_mongo(_snow_ticket, _collection):
            print('ERROR OPENING MONGO TICKET: {}'.format(_snow_ticket))

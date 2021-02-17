import json
import os
from configparser import ConfigParser

import requests
from pymongo import MongoClient


def extract_number_sysids_from_open_snow_tickets_list():
    """
    :return: list
    """
    _open_snow_ticket_numbers_sysids_func_level = []
    # iterate through SNOW json response to obtain ticket numbers. (make function that takes data and pass result)
    for _i in _open_snow_ticket_records.get('result', []):
        if _i.get('u_closed') == 'false':
            _open_snow_ticket_numbers_sysids_func_level.append((_i.get('u_number'),
                                                                _i.get('sys_id'),
                                                                _i.get('u_reporter'),
                                                                _i.get('u_source')))
        else:
            print("WARNING: You found a closed ticket: {}".format(_i.get('u_number', 'UNKNOWN')))
    return _open_snow_ticket_numbers_sysids_func_level


def check_mongo_for_closed_tickets_that_are_open_in_snow(_snow_url_open, _open_snow_ticket_numbers, _headers):
    """
    :param _snow_url_open:
    :param _open_snow_ticket_numbers:
    :param _headers:
    :return: tuple: list, int
    """
    _counter = 0
    _messages = []
    for _ticket in _open_snow_ticket_numbers:
        # getting a single document and set to variable in order to print
        _mongo_result = _collection.find_one({"_id": _ticket[0], "reporter": _ticket[2], "source": _ticket[3]})
        if _mongo_result is not None:
            if _mongo_result.get('phishstory_status') == 'CLOSED':
                _message = close_snow_tickets(_snow_url_open,
                                              _ticket,
                                              _mongo_result.get('closed'),
                                              _headers)
                if isinstance(_message, str) and _message.startswith('Closing'):
                    _counter += 1
                    _messages.append(_message)
    return _messages, _counter


def close_snow_tickets(_snow_url_open, _ticket, _close_date, _headers):
    """
    :param _snow_url_open:
    :param _ticket:
    :param _close_date:
    :param _headers:
    :return: string
    """
    _tix_num, _sys_id, _reporter, _source = _ticket
    _date_str = str(_close_date).split('.')[0]
    _url = '{}/{}'.format(_snow_url_open, _sys_id)
    # DO NOT change the syntax of message below, as the function above
    #  verifies that it starts with the string 'Closing'
    _message = 'Closing ticket {} with a close date of {}'.format(_tix_num, _date_str)
    _my_data = '{"u_closed":"true", "u_closed_date":"%s"}' % _date_str
    _snow_close_response = requests.put(_url,
                                        auth=(_settings.get('snow_user'), _settings.get('snow_pass')),
                                        headers=_headers,
                                        data=_my_data)
    if _snow_close_response.status_code != 200:
        _message = 'Status:', _snow_close_response.status_code, \
                   'Headers:', _snow_close_response.headers, \
                   'Error Response:', _snow_close_response.json()
    return _message


def write_to_slack(_endpoint, _channel, _message_list, _counter):
    """
    :param _endpoint:
    :param _channel:
    :param _message_list:
    :param _counter:
    :return: None
    """
    if len(_message_list):
        _message = '<!here> Closed {} SNOW tickets:'.format(_counter) + '\n'
        _payload = {
            'payload': json.dumps(
                {
                    'channel': _channel,
                    'username': 'API BOT',
                    'text': _message + '\n'.join(_message_list)
                }
            )
        }
        requests.post(_endpoint, data=_payload)


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

    # Set proper SNOW request headers headers
    _headers = {"Content-Type": "application/json", "Accept": "application/json"}

    # Do the HTTP request to SNOW getting all OPEN ticket records
    _response = requests.get(_settings.get('snow_url_open'),
                             auth=(_settings.get('snow_user'), _settings.get('snow_pass')),
                             headers=_headers)

    # Check for HTTP response codes from SNOW for other than 200
    if _response.status_code != 200:
        print('Status:', _response.status_code,
              'Headers:', _response.headers,
              'Error Response:', _response.json())
        exit()

    # Decode the SNOW JSON response into a dictionary and use the data
    # Structure of open_snow_ticket_records looks like:
    # {
    #    'result': [{
    #       'u_target': 'Salesforce',
    #       'u_reporter': '129092584',
    #       'sys_mod_count': '0',
    #       'u_info': '',
    #       'u_notes': '',
    #       'sys_updated_by': 'dcuapi',
    #       'sys_created_by': 'dcuapi',
    #       'sys_id': '00418cec371b6e80362896d543990ef8',
    #       'u_ticket_duration': '',
    #       'u_source_domain_or_ip': 'globalspectrumltd.com',
    #       'sys_tags': '',
    #       'u_number': 'DCU000025632',
    #       'u_url_more_info': '',
    #       'sys_updated_on': '2016-11-21 12:38:02',
    #       'u_proxy_ip': '',
    #       'u_intentional': 'false',
    #       'u_closed_date': '',
    #       'u_group': {
    #           'link': 'https://godaddy.service-now.com/api/now/table/sys_user_group/4b80f9c10f1b8e009d232ca8b1050e20',
    #           'value': '4b80f9c10f1b8e009d232ca8b1050e20'
    #       },
    #       'sys_created_on': '2016-11-21 12:38:02',
    #       'u_closed': 'false',
    #       'u_type': 'PHISHING',
    #       'u_source': 'http://globalspectrumltd.com/media/altweb/'
    #    }]
    # }
    _open_snow_ticket_records = _response.json()

    # All OPEN SNOW tickets
    # Structure of open_snow_ticket_numbers_sysids looks like:
    # [
    #     ('DCU000025632', '00418cec371b6e80362896d543990ef8',
    #      '129092584', 'http://globalspectrumltd.com/media/altweb/'),
    #     ('DCU000026599', '00c158932b17a2407aa46ab3e4da15b3', '129092585',
    #      'http://globalspectrumltd.com/media/altweb/1')
    # ]
    _open_snow_ticket_numbers_sysids = extract_number_sysids_from_open_snow_tickets_list()

    _list_o_messages, _closed_ticket_counter = \
        check_mongo_for_closed_tickets_that_are_open_in_snow(_settings.get('snow_url_open'),
                                                             _open_snow_ticket_numbers_sysids,
                                                             _headers)

    if len(_list_o_messages) > 0:
        write_to_slack(_settings.get('slack_url'),
                       _settings.get('slack_channel'),
                       _list_o_messages,
                       _closed_ticket_counter)

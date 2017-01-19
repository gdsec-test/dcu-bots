import os
from ConfigParser import SafeConfigParser

import requests
from pymongo import MongoClient


def open_ticket_in_mongo(open_snow_ticket):
    tix_num, sys_id = open_snow_ticket
    # Setting the phishstory_status field to OPEN and then removing the close_reason and closed fields altogether
    try:
        collection.update_one({'_id': tix_num}, {'$set': {'phishstory_status': 'OPEN'}}, upsert=False)
        collection.update_one({'_id': tix_num}, {'$unset': {'close_reason': 1, 'closed': 1}}, upsert=False)
        return True
    except Exception as e:
        print('MONGO Exception: {}'.format(e.message))
        return False


def open_snow_tickets(ticket):
    tix_num, sys_id = ticket
    url = 'https://godaddy.service-now.com/api/now/table/u_dcu_ticket/{}'.format(sys_id)
    # DO NOT change the syntax of message below, as the function above
    message = 'Opening ticket {}'.format(tix_num)
    my_data = '{"u_closed":"false", "u_closed_date":""}'
    try:
        snow_close_response = requests.put(url,
                                           auth=(settings.get('snow_user'), settings.get('snow_pass')),
                                           headers=headers,
                                           data=my_data)
        if snow_close_response.status_code != 200:
            message = 'Status:', snow_close_response.status_code,\
                      'Headers:', snow_close_response.headers,\
                      'Error Response:', snow_close_response.json()
            print(message)
            return False
        print(message)
        return True
    except Exception as e:
        print('SNOW Exception: {}'.format(e.message))
        return False


def get_all_specified_snow_tickets(specific_snow_ids):
    all_numbers_list = []
    for snow_id in specific_snow_ids:
        all_numbers_list.append('u_number%3D{}'.format(snow_id))

    all_numbers_string = '^OR'.join(all_numbers_list)
    snow_query_string = 'https://godaddy.service-now.com/api/now/table/u_dcu_ticket?sysparm_limit=240000&sysparm_fields=u_number,sys_id&sysparm_query={}'.format(all_numbers_string)
    # Do the HTTP request to SNOW getting all OPEN ticket records
    response = requests.get(snow_query_string,
                            auth=(settings.get('snow_user'), settings.get('snow_pass')),
                            headers=headers)

    # Check for HTTP response codes from SNOW for other than 200
    if response.status_code != 200:
        print('Status:', response.status_code,
              'Headers:', response.headers,
              'Error Response:', response.json())
        exit()

    # Decode the SNOW JSON response into a dictionary and use the data
    return response.json()


def make_snow_sysid_number_list(specific_snow_ids):
    snow_sysid_number_list = []

    # Structure for snow_records looks like:
    # [{
    #     u 'sys_id': u '01eb3e1437c76a00362896d543990e1a',
    #     u 'u_number': u 'DCU000024037'
    # }, {
    #     u 'sys_id': u '034da51e2bc3a24054a41bc5a8da15b1',
    #     u 'u_number': u 'DCU000024766'
    # }]
    snow_records = get_all_specified_snow_tickets(specific_snow_ids)['result']
    for snow_id in snow_records:
        snow_tuple = (snow_id['u_number'], snow_id['sys_id'])
        snow_sysid_number_list.append(snow_tuple)
    return snow_sysid_number_list


if __name__ == '__main__':
    mode = os.getenv('sysenv') or 'dev'
    # mode = 'prod'

    configp = SafeConfigParser()
    dir_path = os.path.dirname(os.path.realpath(__file__))
    configp.read('{}/missed_tickets_settings.ini'.format(dir_path))

    settings = dict(configp.items(mode))

    # creating pymongo settings
    client = MongoClient(settings.get('db_url'))

    # authenticating to client
    client[settings.get('db')].authenticate(settings.get('db_user'),
                                            settings.get('db_pass'),
                                            mechanism=settings.get('db_auth_mechanism'))

    # getting the mongo db
    db = client[settings.get('db')]

    # getting the collection
    collection = db.incidents

    # Set proper SNOW request headers headers
    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    # Read the list of SNOW ticket ids to open
    filename = 'closed_ticket_ids.txt'

    # Structure of closed_snow_ticket_ids looks like:
    # ['DCU000025036', 'DCU000024953']
    closed_snow_ticket_ids = [line.strip() for line in open(filename)]

    # Structure of specific_closed_tickets looks like:
    # [(u 'DCU000024037', u '01eb3e1437c76a00362896d543990e1a'),(u 'DCU000024766', u '034da51e2bc3a24054a41bc5a8da15b1')]
    specific_closed_tickets = make_snow_sysid_number_list(closed_snow_ticket_ids)

    for snow_ticket in specific_closed_tickets:
        if not open_snow_tickets(snow_ticket):
            print('ERROR OPENING SNOW TICKET: {}'.format(snow_ticket))
        if not open_ticket_in_mongo(snow_ticket):
            print('ERROR OPENING MONGO TICKET: {}'.format(snow_ticket))
import json
import os
from ConfigParser import SafeConfigParser

import requests
from pymongo import MongoClient


def extract_number_sysids_from_open_snow_tickets_list():
    open_snow_ticket_numbers_sysids_func_level = []
    # iterate through SNOW json response to obtain ticket numbers. (make function that takes data and pass result)
    for i in open_snow_ticket_records['result']:
        if i['u_closed'] == 'false':
            open_snow_ticket_numbers_sysids_func_level.append((i['u_number'], i['sys_id']))
        else:
            print("WARNING: You found a closed ticket: {}".format(i['u_number']))
    return open_snow_ticket_numbers_sysids_func_level


def check_mongo_for_closed_tickets_that_are_open_in_snow(open_snow_ticket_numbers):
    counter = 0
    messages = []
    for ticket in open_snow_ticket_numbers:
        # getting a single document and set to variable in order to print
        mongo_result = collection.find_one({"_id": ticket[0]})
        if mongo_result is not None:
            if mongo_result['phishstory_status'] == 'CLOSED':
                message = close_snow_tickets(ticket, mongo_result['closed'])
                if message.startswith('Closing'):
                    counter += 1
                messages.append(message)
    return messages, counter


def close_snow_tickets(ticket, close_date):
    tix_num, sys_id = ticket
    date_str = str(close_date).split('.')[0]
    url = 'https://godaddy.service-now.com/api/now/table/u_dcu_ticket/{}'.format(sys_id)
    # DO NOT change the syntax of message below, as the function above
    #  verifies that it starts with the string 'Closing'
    message = 'Closing ticket {} with a close date of {}'.format(tix_num, date_str)
    my_data = '{"u_closed":"true", "u_closed_date":"%s"}' % date_str
    snow_close_response = requests.put(url,
                                       auth=(settings.get('snow_user'), settings.get('snow_pass')),
                                       headers=headers,
                                       data=my_data)
    if snow_close_response.status_code != 200:
        message = 'Status:', snow_close_response.status_code,\
                  'Headers:', snow_close_response.headers,\
                  'Error Response:', snow_close_response.json()
    return message


def write_to_slack(endpoint, channel, message_list, counter):
    if len(message_list):
        message = '<!here> Closed {} SNOW tickets:'.format(counter) + '\n'
        payload = {'payload': json.dumps({
            'channel': channel,
            'username': 'API BOT',
            'text': message + '\n'.join(message_list)
        })
        }
        requests.post(endpoint, data=payload)


if __name__ == '__main__':
    mode = os.getenv('sysenv') or 'dev'

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

    # Do the HTTP request to SNOW getting all OPEN ticket records
    response = requests.get(settings.get('snow_url_open_tickets'),
                            auth=(settings.get('snow_user'), settings.get('snow_pass')),
                            headers=headers)

    # Check for HTTP response codes from SNOW for other than 200
    if response.status_code != 200:
        print('Status:', response.status_code,
              'Headers:', response.headers,
              'Error Response:', response.json())
        exit()

    # Decode the SNOW JSON response into a dictionary and use the data
    # Structure of open_snow_ticket_records looks like:
    # {
    #     u 'result': [{
    #         u 'u_target': u 'Salesforce',
    #         u 'u_reporter': u '129092584',
    #         u 'sys_mod_count': u '0',
    #         u 'u_info': u '',
    #         u 'u_notes': u '',
    #         u 'sys_updated_by': u 'dcuapi',
    #         u 'sys_created_by': u 'dcuapi',
    #         u 'sys_id': u '00418cec371b6e80362896d543990ef8',
    #         u 'u_ticket_duration': u '',
    #         u 'u_source_domain_or_ip': u 'globalspectrumltd.com',
    #         u 'sys_tags': u '',
    #         u 'u_number': u 'DCU000025632',
    #         u 'u_url_more_info': u '',
    #         u 'sys_updated_on': u '2016-11-21 12:38:02',
    #         u 'u_proxy_ip': u '',
    #         u 'u_intentional': u 'false',
    #         u 'u_closed_date': u '',
    #         u 'u_group': {
    #             u 'link': u 'https://godaddy.service-now.com/api/now/table/sys_user_group/4b80f9c10f1b8e009d232ca8b1050e20',
    #             u 'value': u '4b80f9c10f1b8e009d232ca8b1050e20'
    #         },
    #         u 'sys_created_on': u '2016-11-21 12:38:02',
    #         u 'u_closed': u 'false',
    #         u 'u_type': u 'PHISHING',
    #         u 'u_source': u 'http://globalspectrumltd.com/media/altweb/'
    #     }]
    # }
    open_snow_ticket_records = response.json()

    # All OPEN SNOW tickets
    # Structure of open_snow_ticket_numbers_sysids looks like:
    # [
    #     (u 'DCU000025632', u '00418cec371b6e80362896d543990ef8'),
    #     (u 'DCU000026599', u '00c158932b17a2407aa46ab3e4da15b3')
    # ]
    open_snow_ticket_numbers_sysids = extract_number_sysids_from_open_snow_tickets_list()

    list_o_messages, closed_ticket_counter = \
        check_mongo_for_closed_tickets_that_are_open_in_snow(open_snow_ticket_numbers_sysids)

    if len(list_o_messages) > 0:
        write_to_slack(settings.get('slack_url'),
                       settings.get('slack_channel'),
                       list_o_messages,
                       closed_ticket_counter)
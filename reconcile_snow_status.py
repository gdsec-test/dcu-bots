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
    response = requests.get(settings.get('snow_url'),
                            auth=(settings.get('snow_user'), settings.get('snow_pass')),
                            headers=headers)

    # Check for HTTP response codes from SNOW for other than 200
    if response.status_code != 200:
        print('Status:', response.status_code,
              'Headers:', response.headers,
              'Error Response:', response.json())
        exit()

    # Decode the SNOW JSON response into a dictionary and use the data
    open_snow_ticket_records = response.json()

    # All OPEN SNOW tickets
    open_snow_ticket_numbers_sysids = extract_number_sysids_from_open_snow_tickets_list()

    list_o_messages, closed_ticket_counter = \
        check_mongo_for_closed_tickets_that_are_open_in_snow(open_snow_ticket_numbers_sysids)

    if len(list_o_messages) > 0:
        write_to_slack(settings.get('slack_url'),
                       settings.get('slack_channel'),
                       list_o_messages,
                       closed_ticket_counter)

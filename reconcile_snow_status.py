import json
import os
from ConfigParser import SafeConfigParser

import requests
from pymongo import MongoClient


def get_snow_tickets():
    open_snow_tickets = []
    # iterate through SNOW json response to obtain ticket numbers. (make function that takes data and pass result)
    for i in data['result']:
        if i['u_closed'] == 'false':
            open_snow_tickets.append((i['u_number'], i['sys_id']))
        else:
            print("WARNING: You found a closed ticket: {}".format(i['u_number']))
    return open_snow_tickets


def check_mongo(ticket_numbers):
    counter = 0
    messages = ""
    for ticket in ticket_numbers:
        # getting a single document and set to variable in order to print
        mongo_result = collection.find_one({"_id": ticket[0]})
        if mongo_result is not None:
            if mongo_result['phishstory_status'] == 'CLOSED':
                messages += _close_snow_tickets(ticket, mongo_result['closed'])
                counter += 1
    return messages


def _close_snow_tickets(ticket, close_date):
    tix_num, sys_id = ticket
    date_str = str(close_date).split('.')[0]
    url = 'https://godaddy.service-now.com/api/now/table/u_dcu_ticket/{}'.format(sys_id)
    message = 'Closing ticket {} with a close date of {}'.format(tix_num, date_str)
    my_data = '{"u_closed":"true", "u_closed_date":"%s"}' % (date_str)
    response = requests.put(url,
                            auth=(settings.get('snow_user'), settings.get('snow_pass')),
                            headers=headers,
                            data=my_data)
    if response.status_code != 200:
        message = 'Status:', response.status_code, 'Headers:', response.headers, 'Error Response:', response.json()
    return message


def write_to_slack(endpoint, channel, mdata):
    if len(mdata):
        message = '<!here> Closed {} SNOW tickets:'.format(len(mdata)) + '\n'
        payload = {'payload': json.dumps({
            'channel': channel,
            'username': 'API BOT',
            'text': message + mdata
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
    client[settings.get('db')].authenticate(settings.get('db_user'), settings.get('db_pass'),
                                            mechanism=settings.get('db_auth_mechanism'))

    # getting the mongo db
    db = client[settings.get('db')]

    # getting the collection
    collection = db.incidents

    # Set proper SNOW request headers headers
    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    # Do the HTTP request to SNOW
    response = requests.get(settings.get('snow_url'), auth=(settings.get('snow_user'), settings.get('snow_pass')),
                            headers=headers)

    # Check for HTTP response codes from SNOW for other than 200
    if response.status_code != 200:
        print('Status:', response.status_code, 'Headers:', response.headers, 'Error Response:', response.json())
        exit()

    # Decode the SNOW JSON response into a dictionary and use the data
    data = response.json()

    # All OPEN SNOW tickets
    ticket_numbers = get_snow_tickets()

    messages = check_mongo(ticket_numbers)
    if len(messages) > 0:
        write_to_slack(settings.get('slack_url'), settings.get('slack_channel'), messages)
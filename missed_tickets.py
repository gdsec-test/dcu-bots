import requests
import os
from celery import Celery
from celeryconfig import CeleryConfig
from pymongo import MongoClient
from ConfigParser import SafeConfigParser


def get_snow_tickets():
    open_snow_tickets = []
    # iterate through SNOW json response to obtain ticket numbers. (make function that takes data and pass result)
    for i in data['result']:
        open_snow_tickets.append(i['u_number'])
    return open_snow_tickets


def check_mongo(ticket_numbers):
    tickets_for_celery = []
    for ticket in ticket_numbers:
        # getting a single document and set to variable in order to print
        mongo_result = collection.find_one({"_id": ticket})
        if mongo_result is None:
            tickets_for_celery.append(ticket)
    return tickets_for_celery


def data_for_celery(celery_tickets):
    list_for_middleware = []
    for child in data['result']:
        for ticket in celery_tickets:
            if child['u_number'] == ticket:
                print ticket
                dict_for_middleware = {'ticketId': ticket,
                                       'type': child['u_type'],
                                       'source': child['u_source'],
                                       'sourceDomainOrIp': child['u_source_domain_or_ip'],
                                       'target': child['u_target'],
                                       'proxy': child['u_proxy_ip'],
                                       'reporter': child['u_reporter']
                                       }
                list_for_middleware.append(dict_for_middleware)
                break
    return list_for_middleware


def pass_to_middleware(list_for_middleware, settings):
    # Celery setup
    print ('Number of missing tickets: {}'.format(len(list_for_middleware)))

    # Un-comment the following "return" in order to see the current number of missing tickets without sending them to middleware
    # return
    capp = Celery()
    capp.config_from_object(CeleryConfig(settings))
    for dictionary in list_for_middleware:
        capp.send_task(settings.get('celery_task'), (dictionary,))


if __name__ == '__main__':
    mode = os.getenv('sysenv') or 'dev'

    configp = SafeConfigParser()
    configp.read('missed_tickets_settings.ini')

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

    ticket_numbers = get_snow_tickets()
    celery_tickets = check_mongo(ticket_numbers)
    list_for_middleware = data_for_celery(celery_tickets)
    pass_to_middleware(list_for_middleware, settings)

import requests
from celery import Celery
from celeryconfig import CeleryConfig
from pymongo import MongoClient


# creating pymongo settings
# set client connection
# client = MongoClient('mongodb://10.22.188.208:27017/')
client = MongoClient('mongodb://10.22.9.209:27017/')

# authenticating to client
# client.devphishstory.authenticate('devuser', 'phishstory', mechanism='SCRAM-SHA-1')
client.phishstory.authenticate('sau_p_phish', 'qn9ddQhu55duSCx', mechanism='MONGODB-CR')

# getting the mongo db
# db = client.devphishstory
db = client.phishstory


# getting the collection
collection = db.incidents


# Set the SNOW request parameters (changed to limit 2 tickets temp testing)
# url = 'https://godaddydev.service-now.com/api/now/table/u_dcu_ticket?sysparm_query=u_closed%3Dfalse%5Esys_created_onRELATIVELE%40hour%40ago%4072&sysparm_limit=15'
url = 'https://godaddy.service-now.com/api/now/table/u_dcu_ticket?sysparm_query=u_closed%3Dfalse%5Esys_created_onRELATIVELE%40hour%40ago%4072&sysparm_limit=20000'


# SNOW DEV creds
# user = 'dcuapi'
# pwd = 'fQSNS24etPez'

# SNOW PROD creds
user = 'dcuapi'
pwd = 'fQSNS24etPez'

# Set proper SNOW request headers headers
headers = {"Content-Type": "application/json", "Accept": "application/json"}

# Do the HTTP request to SNOW
response = requests.get(url, auth=(user, pwd), headers=headers)

# Check for HTTP response codes from SNOW for other than 200
if response.status_code != 200:
    print('Status:', response.status_code, 'Headers:', response.headers, 'Error Response:', response.json())
    exit()

# Decode the SNOW JSON response into a dictionary and use the data
data = response.json()


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


def pass_to_middleware(list_for_middleware):
    # Celery setup
    # api_queue = 'devdcumiddleware'
    print ('Number of missing tickets: {}'.format(len(list_for_middleware)))
    api_queue = 'dcumiddleware'
    api_task = 'run.process'
    capp = Celery()
    capp.config_from_object(CeleryConfig(api_task, api_queue))
    for dictionary in list_for_middleware:
        capp.send_task(api_task, (dictionary,))

ticket_numbers = get_snow_tickets()

celery_tickets = check_mongo(ticket_numbers)

list_for_middleware = data_for_celery(celery_tickets)

pass_to_middleware(list_for_middleware)

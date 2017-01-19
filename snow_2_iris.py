import json
import logging
import requests
import socket
import os

from ConfigParser import SafeConfigParser
from datetime import datetime
from suds.client import Client

mode = os.getenv('sysenv') or 'dev'

configp = SafeConfigParser()
dir_path = os.path.dirname(os.path.realpath(__file__))
configp.read('{}/snow_2_iris_settings.ini'.format(dir_path))

settings = dict(configp.items(mode))

# setting up a console handler for logging
logging.basicConfig(level=logging.INFO)

# enabling debugging for suds.client to show sent/received soap messages
logging.getLogger('suds.client').setLevel(logging.INFO)

# WSDL (web services description file) for IRIS Webservice is
wsdl_url = settings.get('wsdl_url')

# create a client for RegDBWbSvcService WSDL
client = Client(wsdl_url)

# Gets machine IP
ip = socket.gethostbyname(socket.gethostname())

# SNOW URL to obtain all open CHILD_ABUSE and CONTENT TICKET TYPES
snow_url = settings.get('snow_url')

# SNOW CREDS
snow_user = settings.get('snow_user')
snow_pass = settings.get('snow_pass')

# Set proper SNOW request headers headers
headers = {"Content-Type": "application/json", "Accept": "application/json"}

# Do the HTTP request to SNOW
response = requests.get(snow_url, auth=(snow_user, snow_pass), headers=headers)


# Create a list of tickets that need to be sent to IRIS.  Returns tuple of two lists
# Also creates list of SNOW ticket numbers and sys_ids of tickets being sent to IRIS so they can be closed in SNOW
# Tickets translated to more readable dict with only necessary data
def data_for_iris():
    # List of the ticket dictionaries from dict_for_iris.  Will be sent to IRIS in send_to_iris method
    list_for_iris = []
    # List of the ticket dictionaries from dict_for_closing.  Will be used in close_snow_tickets method
    list_for_closing = []
    for child in data['result']:
        # Dictionary containing only the specified ticket data for a single ticket from data.  Each pass is another ticket
        dict_for_iris = {'snowTicket': child['u_number'],
                         'type': child['u_type'],
                         'source': child['u_source'],
                         'sourceDomainOrIp': child['u_source_domain_or_ip'],
                         'target': child['u_target'],
                         'proxy': child['u_proxy_ip'],
                         'notes': child['u_notes'],
                         'urlMoreInfo': child['u_url_more_info'],
                         'reporter': child['u_reporter']
                         }
        list_for_iris.append(dict_for_iris)

        # Dictionary containing only the specified ticket data for a single ticket from data.  Each pass is another ticket
        dict_for_closing = {'tix_num': child['u_number'], 'sys_id': child['sys_id']}
        list_for_closing.append(dict_for_closing)

    return (list_for_iris, list_for_closing)


# Accepts list_for_closing from data_for_iris method.  Closes SNOW tickets.  Returns a list of messages from ticket closing
def close_snow_tickets(list_for_closing):
    messages = []
    for child in list_for_closing:
        close_date = datetime.utcnow()
        date_str = str(close_date).split('.')[0]
        url = 'https://godaddydev.service-now.com/api/now/table/u_dcu_ticket/{}'.format(child['sys_id'])
        # DO NOT change the syntax of message below, as the function above
        #  verifies that it starts with the string 'Closing'
        message = 'Closing ticket {} with a close date of {}'.format(child['tix_num'], date_str)
        my_data = '{"u_closed":"true", "u_closed_date":"%s"}' % date_str
        snow_close_response = requests.put(url,
                                           auth=(snow_user, snow_pass),
                                           headers=headers,
                                           data=my_data)
        messages.append(message)

        if snow_close_response.status_code != 200:
            message = 'Status:', snow_close_response.status_code,\
                      'Headers:', snow_close_response.headers,\
                      'Error Response:', snow_close_response.json()
            messages.append(message)
    return messages


# Accepts first item in the tuple from data_for_iris.  Returns a list of ticket numbers that were created in IRIS.
# Method to make SOAP calls to create IRIS tickets for CONTENT or CHILD_ABUSE complaints originally submitted to SNOW
def send_to_iris(snow_tickets):
    messages = []
    for i in snow_tickets:
        if i['type'] == 'CONTENT':
            ticket_info = '\n'.join('{}: {}'.format(key, val) for key, val in i.items())
            if mode == 'prod':
                # PROD IRIS insert statement with proper Group/Service IDs for PROD Environment for CONTENT type
                result = client.service.CreateIncidentInIRISByType('436', 'Content Complaints Report', ticket_info,
                                                                   'dcu@godaddy.com', ip, '432', '241', '1', '',
                                                                   'DCU Eng Bot', '26')
            else:
                # DEV IRIS insert statement with proper Group/Service IDs for DEV Environment for CONTENT type
                result = client.service.CreateIncidentInIRISByType('425', 'Content Complaints Report', ticket_info,
                                                                   'dcu@godaddy.com', ip, '504', '227', '1', '',
                                                                   'DCU Eng Bot', '26')
            # print result
            messages.append(result)
        else:
            ticket_info = '\n'.join('{}: {}'.format(key, val) for key, val in i.items())
            if mode == 'prod':
                # PROD IRIS insert statement with proper Group/Service IDs for PROD Environment for CHILD_ABUSE type
                result = client.service.CreateIncidentInIRISByType('389', 'Child Abuse Report', ticket_info,
                                                                   'dcu@godaddy.com', ip, '443', '221', '1', '',
                                                                   'DCU Eng Bot', '26')
            else:
                # DEV IRIS insert statement with proper Group/Service IDs for DEV Environment for CHILD_ABUSE type
                result = client.service.CreateIncidentInIRISByType('425', 'Child Abuse Report', ticket_info,
                                                                   'dcu@godaddy.com', ip, '510', '214', '1', '',
                                                                   'DCU Eng Bot', '26')
            # print result
            messages.append(result)

    return messages

# Method to send method response to an alert channel for DCU
def write_to_slack(iris_tickets, closed_tickets, slack_url):
    channel = settings.get('slack_channel')
    if len(iris_tickets):
        message = '<!here> {} IRIS tickets created'.format(len(iris_tickets)) + '\n'
        for i in iris_tickets:
            message += str(i) + '\n'
        payload = {'payload': json.dumps({
            'channel': channel,
            'username': 'DCU Eng BOT',
            'text': message
        })
        }
        requests.post(slack_url, data=payload)

    if len(closed_tickets):
        message = '<!here> {} SNOW tickets closed'.format(len(closed_tickets)) + '\n'
        for i in closed_tickets:
            message += str(i) + '\n'
        payload = {'payload': json.dumps({
            'channel': channel,
            'username': 'DCU Eng BOT',
            'text': message
        })
        }
        requests.post(slack_url, data=payload)


# Check for HTTP response codes from SNOW for other than 200
if response.status_code != 200:
    print('Status:', response.status_code, 'Headers:', response.headers, 'Error Response:', response.json())
    exit()

# Decode the SNOW JSON response into a dictionary and use the data
data = response.json()

# print data
snow_tickets = data_for_iris()
send_tickets = send_to_iris(snow_tickets[0])
messages_from_closing = close_snow_tickets(snow_tickets[1])
write_to_slack(send_tickets, messages_from_closing, settings.get('slack_url'))
'''
Soap UI template used in send_to_iris() above

Subscriber ID, Subject, Note, Customer Email Address, IP, Group ID, Service ID, Private Label ID, Shopper ID, Created By, Incident Type

result = client.service.CreateIncidentInIRISByType('Subscriber ID', 'Subject', Note, 'Customer Email Address', IP,
                                                    'Group ID', 'Service ID', 'Private Label ID', 'Shopper ID',
                                                    'Created By', 'Incident Type')

XXXXXXCreateIncidentInIRISByTypeXXXXXX

      <tem:CreateIncidentInIRISByType>
         <tem:subscriberID>425</tem:subscriberID>
         <!--Optional:-->
         <tem:subject>Test Subject</tem:subject>
         <!--Optional:-->
         <tem:Note>PAYLOAD</tem:Note>
         <!--Optional:-->
         <tem:customerEmailAddress>lhalvorson@godaddy.com</tem:customerEmailAddress>
         <!--Optional:-->
         <tem:originalIPAddress>?</tem:originalIPAddress>
         <tem:groupID>510</tem:groupID>
         <tem:serviceID>214</tem:serviceID>
         <tem:privateLabelID>1</tem:privateLabelID>
         <!--Optional:-->
         <tem:shopperID></tem:shopperID>
         <!--Optional:-->
         <tem:createdBy>DCU-Eng</tem:createdBy>
         <tem:incidentType>26</tem:incidentType>
      </tem:CreateIncidentInIRISByType>
'''

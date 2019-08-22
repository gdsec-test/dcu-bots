import base64
import logging

import requests
import os


class PhishlabsAPI:

    caseType = 'Phishing'
    brand = 'GoDaddy'
    title = 'Godaddy Phish Ticket'
    url_data_api = 'https://caseapi.phishlabs.com/v1/data/cases'
    header_data_api = {'Content-type': 'application/json', 'Accept': 'application/json',
                             'Authorization': 'Basic {}'.format(base64.b64encode('{}:{}'.format(
                                 os.getenv('PHISHLABS_API_USERNAME', 'godaddy.api'),
                                 os.getenv('PHISHLABS_API_PASSWORD', 'password'))))}

    """
    This class handles access to Phishlabs APIs
    """
    def __init__(self):
        self._logger = logging.getLogger(__name__)

    def retrieve_tickets(self, time_start, date_field):
        """
        Retrieves all Phishlabs tickets for the given time frame (time_start to now)
        :param time_start: Start of time frame in UTC format.
        :type time_start: UTC formatted str
        :param date_field: caseModify time. (Created/Closed/Modified).
        :type date_field: str
        :return:
        """

        if type(time_start) not in [str]:
            self._logger.warning('Unable to retrieve PhishLabs ticket. Check time_start value.')
            return None

        payload = {
            'dateBegin': time_start,
            'caseType': self.caseType,
            'brand': self.brand,
            'title': self.title,
            'dateField': date_field,
            'format': 'json'
        }

        data = None
        try:
            response = requests.get(self.url_data_api, headers=self.header_data_api, params=payload)
            if response.status_code == 200:
                data = response.json()
            else:
                self._logger.warning('Unable to retrieve PhishLabs ticket {}'.format(response.content))
        except Exception as e:
            self._logger.error('Exception while retrieving PhishLabs ticket {}'.format(e.message))
        finally:
            return data


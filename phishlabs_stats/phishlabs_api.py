import base64
import logging
import os

import requests


class PhishlabsAPI:
    caseType = 'Phishing'
    brand = 'GoDaddy'
    title = 'Godaddy Phish Ticket'
    url_data_api = 'https://caseapi.phishlabs.com/v1/data/cases'
    header_data_api = {'Content-type': 'application/json', 'Accept': 'application/json',
                       'Authorization': 'Basic {}'.format(base64.b64encode(str.encode('{}:{}'.format(
                           os.getenv('PHISHLABS_API_USERNAME', 'godaddy.api'),
                           os.getenv('PHISHLABS_API_PASSWORD', 'password')))).decode())}

    """
    This class handles access to Phishlabs APIs
    """
    def __init__(self):
        self._logger = logging.getLogger(__name__)

    def retrieve_tickets(self, _time_start, _date_field):
        """
        Retrieves all Phishlabs tickets for the given time frame (time_start to now)
        :param _time_start: Start of time frame in UTC format.
        :type _time_start: UTC formatted str
        :param _date_field: caseModify time. (Created/Closed/Modified).
        :type _date_field: str
        :return:
        """

        if type(_time_start) not in [str]:
            self._logger.warning('Unable to retrieve PhishLabs ticket. Check time_start value.')
            return

        _payload = {
            'dateBegin': _time_start,
            'caseType': self.caseType,
            'brand': self.brand,
            'title': self.title,
            'dateField': _date_field,
            'format': 'json'
        }

        _data = None
        try:
            _response = requests.get(self.url_data_api, headers=self.header_data_api, params=_payload)
            if _response.status_code == 200:
                _data = _response.json()
            else:
                self._logger.warning('Unable to retrieve PhishLabs ticket {}'.format(_response.content))
        except Exception as _e:
            self._logger.error('Exception while retrieving PhishLabs ticket {}'.format(_e))
        finally:
            return _data

import requests
from mock import MagicMock, patch
from nose.tools import assert_equal

from phishlabs_api import PhishlabsAPI


class TestPhishlabsAPIFunctions:
    @classmethod
    def setup_class(cls):
        cls._phishlabsapi = PhishlabsAPI()

    @patch.object(requests, 'get')
    def test_retrieve_tickets_success(self, mock_get):
        response = {'hello': 'bye'}
        mock_get.return_value = MagicMock(status_code=200)
        mock_get.return_value.json.return_value = response
        assert_equal(self._phishlabsapi.retrieve_tickets('1234', 'caseModify'), response)
        mock_get.assert_called()

    @patch.object(requests, 'get', return_value=MagicMock(status_code=400))
    def test_retrieve_tickets_fail(self, mock_get):
        assert_equal(self._phishlabsapi.retrieve_tickets('1234', 'caseModify'), None)
        mock_get.assert_called()

    @patch.object(requests, 'get', return_value=None)
    def test_retrieve_tickets_exception(self, mock_get):
        assert_equal(self._phishlabsapi.retrieve_tickets('1234', 'caseModify'), None)
        mock_get.assert_called()

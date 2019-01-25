import requests
from mock import MagicMock, patch
from nose.tools import assert_equal

from phishlabs_api import PhishlabsAPI


class TestPhishlabsAPIFunctions:
    @classmethod
    def setup_class(cls):
        cls._phishlabsapi = PhishlabsAPI()

    @patch.object(requests, 'get')
    def test_retrieve_tickets_success(self, mocked_method):
        response = {'hello': 'bye'}
        mocked_method.return_value = MagicMock(status_code=200)
        mocked_method.return_value.json.return_value = response
        assert_equal(self._phishlabsapi.retrieve_tickets('1234', 'caseModify'), response)

    @patch.object(requests, 'get')
    def test_retrieve_tickets_fail(self, mocked_method):
        mocked_method.return_value = MagicMock(status_code=400)
        assert_equal(self._phishlabsapi.retrieve_tickets('1234', 'caseModify'), None)

    @patch.object(requests, 'get', return_value=None)
    def test_retrieve_tickets_exception(self, mocked_method):
        assert_equal(self._phishlabsapi.retrieve_tickets('1234', 'caseModify'), None)

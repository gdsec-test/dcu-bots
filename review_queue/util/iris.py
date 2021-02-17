import logging

import pyodbc


class IrisDB:
    def __init__(self, settings):
        self._logger = logging.getLogger(__name__)
        _conn_string = 'DRIVER=FreeTDS;SERVER={server};PORT={port};DATABASE={database};UID={username};PWD={password};TDS_VERSION=8.0'
        self._database_url = _conn_string.format(server=settings.IRIS_SERVER, port=settings.IRIS_PORT,
                                                 database=settings.IRIS_DATABASE, username=settings.IRIS_USERNAME,
                                                 password=settings.IRIS_PASSWORD)

    def _rows(self, _query):
        """
        Initializes a cursor and communicates with the Iris backend for a given query.
        :param _query: The query to execute on the initialized cursor
        """
        _connection = _cursor = None
        try:
            _connection = pyodbc.connect(self._database_url)
            _connection.autocommit = True
            _connection.timeout = 0
            _cursor = _connection.cursor()
            _cursor.execute(_query)
            return _cursor.fetchone()
        except Exception as _e:
            self._logger.error('Error processing query {} {}'.format(_query, _e))
        finally:
            if _cursor:
                _cursor.close()
            if _connection:
                _connection.close()

    def get_review_queue_count(self, _group_id, _service_id):
        """
        Retrieves the total number of incidents from the review queue in Iris
        :param _group_id: The Iris GroupID (Integer)
        :param _service_id: The corresponding Iris Service ID (Integer) for the review queue.
        """
        _open_status = 1
        _suspend_status = 4
        _query = "SELECT COUNT(iris_incidentID) FROM IRISIncidentMain " \
                 "WHERE iris_groupID = '{group_id}' AND (iris_serviceID = '{service_id}') " \
                 "AND iris_statusID IN ({open_status}, {suspend_status})".format(group_id=_group_id,
                                                                                 service_id=_service_id,
                                                                                 open_status=_open_status,
                                                                                 suspend_status=_suspend_status)

        _report_count_tuple = self._rows(_query)
        if isinstance(_report_count_tuple, pyodbc.Row) and len(_report_count_tuple) > 0:
            return _report_count_tuple[0]

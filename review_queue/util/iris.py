import logging
import pyodbc


class IrisDB:
    def __init__(self, settings):
        self._logger = logging.getLogger(__name__)
        self._connection_string = 'DRIVER=FreeTDS;SERVER={server};PORT={port};DATABASE={database};UID={username};PWD={password};TDS_VERSION=8.0'
        self._database_url = self._connection_string.format(server=settings.IRIS_SERVER, port=settings.IRIS_PORT, database=settings.IRIS_DATABASE,
                                                            username=settings.IRIS_USERNAME, password=settings.IRIS_PASSWORD)

    def _rows(self, query):
        """
        Initializes a cursor and communicates with the Iris backend for a given query.
        :param query: The query to execute on the initialized cursor
        """
        connection = None
        cursor = None
        try:
            connection = pyodbc.connect(self._database_url)
            connection.autocommit = True
            connection.timeout = 0
            cursor = connection.cursor()
            cursor.execute(query)
            return cursor.fetchone()
        except Exception as e:
            self._logger.error('Error processing query {} {}'.format(query, e.message))
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def get_review_queue_count(self, group_id, service_id):
        """
        Retrieves the total number of incidents from the review queue in Iris
        :param group_id: The Iris GroupID (Integer)
        :param service_id: The corresponding Iris Service ID (Integer) for the review queue.
        """
        open_status = 1
        suspend_status = 4
        query = "SELECT COUNT(iris_incidentID) FROM IRISIncidentMain " \
                "WHERE iris_groupID = '{group_id}' AND (iris_serviceID = '{service_id}') " \
                "AND iris_statusID IN ({open_status}, {suspend_status})".format(group_id=group_id,
                                                                                service_id=service_id,
                                                                                open_status=open_status,
                                                                                suspend_status=suspend_status)

        report_count_tuple = self._rows(query)
        if isinstance(report_count_tuple, pyodbc.Row) and len(report_count_tuple) > 0:
            return report_count_tuple[0]

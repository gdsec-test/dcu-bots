import os


class AppConfig(object):
    IRIS_WSDL = None
    IRIS_SERVER = None
    IRIS_PORT = None

    IRIS_DATABASE = 'iris'

    IRIS_SERVICE_ID_REVIEW = None
    IRIS_GROUP_ID_CSA = None

    def __init__(self):
        self.IRIS_USERNAME = os.getenv('IRIS_USERNAME')
        self.IRIS_PASSWORD = os.getenv('IRIS_PASSWORD')


class ProductionAppConfig(AppConfig):
    IRIS_WSDL = "https://iris-ws.int.godaddy.com/iriswebservice.asmx?WSDL"
    IRIS_SERVER = '10.32.146.30'
    IRIS_PORT = 1433

    IRIS_SERVICE_ID_REVIEW = 227
    IRIS_GROUP_ID_CSA = 443

    def __init__(self):
        super(ProductionAppConfig, self).__init__()


class DevelopmentAppConfig(AppConfig):
    IRIS_WSDL = "https://iris-ws.dev.int.godaddy.com/iriswebservice.asmx?WSDL"
    IRIS_SERVER = '10.32.76.23\\CSS'

    IRIS_SERVICE_ID_REVIEW = 219
    IRIS_GROUP_ID_CSA = 510

    def __init__(self):
        super(DevelopmentAppConfig, self).__init__()


config_by_name = {'dev': DevelopmentAppConfig, 'prod': ProductionAppConfig}

import pytest
from pysc.DTC_Client_Service import DtcClientService, ApiNotAuthorised, ApiNoHistoricalData
from pysc.SC_History_client import SCHistoryClient
from server import historical_server, market_data_server

class TestHistoricalPriceDataRequest:

    def test_good_download(self):
        dtc_connection = SCHistoryClient(historical_server, encoding='protobuf')
        df = dtc_connection.historical_data_request("CCN15-ICEUS",'', 'H', start=0, end=0)

        assert dtc_connection.logged_on is False
        assert len(df) == 581

    def test_not_authorised_download(self):
        dtc_connection = SCHistoryClient(historical_server, encoding='protobuf')

        exception_caught = False
        try:
            df = dtc_connection.historical_data_request("6AM22-ICEUS",'', 'H', start=0, end=0)
        except ApiNotAuthorised:
            exception_caught = True

        assert exception_caught

    def test_no_historical_data(self):
        dtc_connection = SCHistoryClient(historical_server, encoding='protobuf')

        exception_caught = False
        try:
            df = dtc_connection.historical_data_request("CCN05-ICEUS",'', 'H', start=0, end=0)
        except ApiNoHistoricalData:
            exception_caught = True

        assert exception_caught

    def test_unknown_symbol(self):
        dtc_connection = SCHistoryClient(historical_server, encoding='protobuf')

        exception_caught = False
        try:
            df = dtc_connection.historical_data_request("WANGDOODLE",'', 'H', start=0, end=0)
        except ApiNoHistoricalData:
            exception_caught = True

        assert exception_caught

if __name__=="__main__":
    pytest.main(["-x", "test"])

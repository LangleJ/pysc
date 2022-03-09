import os
import pytest
from pysc.SC_MarketData_Client import SCMarketDataClient, ApiInvalidSymbol
from server import historical_server, market_data_server

class TestSecurityDefinition:

    def test_good_response(self):
        dtc_connection = SCMarketDataClient(market_data_server)
        result = dtc_connection.securityDefinitionForSymbol('CCN15-ICEUS')
        dtc_connection.logout()
        assert len(result[0].keys()) == 14

    def test_bad_response(self):
        dtc_connection = SCMarketDataClient(market_data_server)
        # We expect this test to fail because either no response or len = 8 response
        exception_caught = False
        try:
            result = dtc_connection.securityDefinitionForSymbol('Wangdoodle')
        except ApiInvalidSymbol:
            exception_caught = True
        dtc_connection.logout()
        assert exception_caught


if __name__=="__main__":
    pytest.main(["-x", "test"])

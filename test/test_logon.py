import pytest
from pysc.DTC_Client_Service import DtcClientService
from server import historical_server, market_data_server

class TestLogon:

    def test_logon_logoff_protobuf(self):
        dtc_connection = DtcClientService(historical_server, encoding='protobuf', heartbeat=True)
        dtc_connection.login()
        dtc_connection.logout()

if __name__=="__main__":
    pytest.main(["-x", "test"])
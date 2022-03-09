from pysc.DTC_Client_Service import *

class SCHistoryClient(DtcClientService):
    """
    A wrapper around DtcClientService that manages a connection to a local 
    SierraChart Historical Data server
    
    Behaviour is: login, get history, logout
    Overrides ctor args to NOT use heartbeats
    """

    def __init__(self, *args, **kwargs):
        """
        """
        kwarg_update = {'heartbeat':False}
        kwargs.update(kwarg_update)
        super(SCHistoryClient, self).__init__(*args, **kwargs)
        return

    def historical_data_request(self, symbol, exchange, interval, start=0, end=0):
        """
        """
        self.login()
        try:
            df = super(SCHistoryClient, self).historical_data_request(symbol, exchange, interval, start, end)
            self.logout()
            return df
        except BaseException as exc:
            self.logout()
            raise
            
        return


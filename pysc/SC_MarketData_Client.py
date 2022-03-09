from pysc.DTC_Client_Service import *

class SCMarketDataClient(DtcClientService):
    """
    A wrapper around DtcClientService that manages a connection to a local 
    SierraChart Market Data server
    If we're logged out when we make a request, auto log back in again
    Overrides ctor args to use heartbeats on 20 second intrval
    """
    def __init__(self, *args, **kwargs):
        kwarg_update = {'heartbeat':True}
        kwargs.update(kwarg_update)
        super(SCMarketDataClient, self).__init__(*args, **kwargs)
        self.login()
        return

    def securityDefinitionForSymbol(self, symbol, exchange=''):
        result = super(SCMarketDataClient, self).securityDefinitionForSymbol(symbol, exchange)
        return result

    def SymbolsForUnderlyingRequest(self, symbol, exchange='', securityType='futures'):
        result = super(SCMarketDataClient, self).SymbolsForUnderlyingRequest(symbol, exchange, securityType)
        return result

    def SymbolSearchRequest(self, symbol, exchange='', securityType='futures'):
        result = super(SCMarketDataClient, self).SymbolSearchRequest(symbol, exchange, securityType)
        return result

    def UnderlyingSymbolsForExchangeRequest(self, exchange='', securityType='futures'):
        result = super(SCMarketDataClient, self).SymbolSearchRequest(exchange, securityType)
        return result
from pysc.DTC.DTCProtocol_pb2 import *

# Mapping message type to DTC message object and human readable name
DTC_MTYPE_MAP = {
    ENCODING_REQUEST: (EncodingRequest, "encoding request"),
    ENCODING_RESPONSE: (EncodingResponse, "encoding response"),
    LOGON_REQUEST: (LogonRequest, "logon request"),
    LOGON_RESPONSE: (LogonResponse, "logon response"),
    HEARTBEAT: (Heartbeat, "heartbeat"),
    LOGOFF: (Logoff, "logoff"),
    GENERAL_LOG_MESSAGE: (GeneralLogMessage, "general log message"),
    MARKET_DATA_SNAPSHOT: (MarketDataSnapshot, "market data snapshot"),

    HISTORICAL_PRICE_DATA_REQUEST: (HistoricalPriceDataRequest, "historical data request"),
    HISTORICAL_PRICE_DATA_REJECT: (HistoricalPriceDataReject, "historical data reject"),
    HISTORICAL_PRICE_DATA_RESPONSE_HEADER: (HistoricalPriceDataResponseHeader, "historical data response header"),
    HISTORICAL_PRICE_DATA_RECORD_RESPONSE: (HistoricalPriceDataRecordResponse,"historical data record response"),

    UNDERLYING_SYMBOLS_FOR_EXCHANGE_REQUEST  :(UnderlyingSymbolsForExchangeRequest, "Underlying Symbols For Exchange Request"),
    SYMBOL_SEARCH_REQUEST  : (SymbolSearchRequest, "Symbol Search Request"),
    SYMBOLS_FOR_UNDERLYING_REQUEST : (SymbolsForUnderlyingRequest, "Symbols For Underlying Request"),
    SECURITY_DEFINITION_FOR_SYMBOL_REQUEST: (SecurityDefinitionForSymbolRequest ,"Security definition for symbol request"),
    SECURITY_DEFINITION_REJECT : (SecurityDefinitionReject ,"Security definition for symbol reject"),
    SECURITY_DEFINITION_RESPONSE : (SecurityDefinitionResponse ,"Security definition for symbol response")
}


def DtcMsgObjectFromType(msg_type):
    entry = DTC_MTYPE_MAP[msg_type]
    return entry[0]()

def DtcMsgNameFromType(msg_type):
    return DTC_MTYPE_MAP[msg_type][1]

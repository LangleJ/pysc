from struct import pack, unpack, unpack_from, calcsize
from pysc.DTC.DTCProtocol_binary import *  # import DTCProtocol_binary, then re-define in this file any functions listed in DTCProtocolVLS.h
del DTC_MTYPE_MAP                           # We dont want to map from DTCProtocol_binary

# ENUMS - we get from DTCProtocol_binary imported above

class s_vlsf:
    Offset =0
    Length =0
    Format =''
    Bytes  =b''

def AddVariableLengthStringField(r_BaseStructureSizeField, string):
    SizeToAdd = len(string)
    r_VariableLengthStringField = s_vlsf()
    if SizeToAdd == 0:
        r_VariableLengthStringField.Offset = 0
        r_VariableLengthStringField.Length = 0
    else:
        r_VariableLengthStringField.Offset = r_BaseStructureSizeField   # This should be 0 I think, not 2
        r_VariableLengthStringField.Length = SizeToAdd + 1
        r_BaseStructureSizeField += r_VariableLengthStringField.Length

    r_VariableLengthStringField.Format =f'<{SizeToAdd+1}s'
    r_VariableLengthStringField.Bytes = (string + '0').encode('UTF-8')
    return r_BaseStructureSizeField, r_VariableLengthStringField

def GetVariableLengthStringField(Message, BaseStructureSizeField, VariableLengthStringField, VariableLengthStringFieldOffset):
    sizeofvls_t =4
    MessageSizeField = len(Message)
    bytes_missing = 4
    if (BaseStructureSizeField < VariableLengthStringFieldOffset + sizeofvls_t):
        return ""
    elif (VariableLengthStringField.Offset - bytes_missing == 0 or VariableLengthStringField.Length <= 1):
        return ""
    elif ((VariableLengthStringField.Offset - bytes_missing + VariableLengthStringField.Length) > MessageSizeField):
        return ""
    else:
        Length = VariableLengthStringField.Length
        if (Length > 4096):
            Length = 4096
            
        field = Message[VariableLengthStringField.Offset - bytes_missing  : VariableLengthStringField.Offset - bytes_missing + Length -1]
        msg = field.decode('UTF-8')
        return msg




class LogonRequest():
    _fmt = '<|H|i|HH|HH|HH|i|i|i|i|HH|HH|HH|i'
    BaseSize = 0x38
    ProtocolVersion = 0x08
    Username = ''
    Password = ''
    GeneralTextData = ''
    Integer_1 = 0
    Integer_2 = 0
    HeartbeatIntervalInSeconds = 0
    TradeModeEnum = 0
    TradeAccount = ''
    HardwareIdentifier = ''
    ClientName = ''
    MarketDataTransmissionInterval = 0

    def SerializeToString(self,):
        #fmt_padded = padder(self._fmt)
        fmt_padded = '<HxxiHHHHHHiiiiHHHHHHi'
        fmt_size = calcsize(fmt_padded)

        bss = self.BaseSize = fmt_size + 4
        bss, vlsUsername            = AddVariableLengthStringField(bss, self.Username)
        bss, vlsPassword            = AddVariableLengthStringField(bss, self.Password)
        bss, vlsGeneralTextData     = AddVariableLengthStringField(bss, self.GeneralTextData)
        bss, vlsTradeAccount        = AddVariableLengthStringField(bss, self.TradeAccount)
        bss, vlsHardwareIdentifier  = AddVariableLengthStringField(bss, self.HardwareIdentifier)
        bss, vlsClientName          = AddVariableLengthStringField(bss, self.ClientName)

        msg = pack(padded_fmt,
            self.BaseSize,                                       # 0  H  uint16_t BaseSize;
            self.ProtocolVersion,                                # 2  i  int32_t ProtocolVersion;
            vlsUsername.Offset,                                  # 6  H  vls_t Username;
            vlsUsername.Length,                                  # 10 H  vls_t Username;
            vlsPassword.Offset,                                  # 14 H  vls_t Password;
            vlsPassword.Length,                                  # 18 H vls_t Password;
            vlsGeneralTextData.Offset,                           # 22 H  vls_t GeneralTextData;
            vlsGeneralTextData.Length,                           # 26 H  vls_t GeneralTextData;
            self.Integer_1,                                      # 30 i  int32_t Integer_1;
            self.Integer_2,                                      # 34 i  int32_t Integer_2;
            self.HeartbeatIntervalInSeconds,                     # 38 i  int32_t HeartbeatIntervalInSeconds; 
            self.TradeModeEnum,                                  # 42 i  DTC::TradeModeEnum TradeMode;
            vlsTradeAccount.Offset,                              # 46 H  vls_t TradeAccount; 
            vlsTradeAccount.Length,                              # 50 H  vls_t TradeAccount; 
            vlsHardwareIdentifier.Offset,                        # 54 H  vls_t HardwareIdentifier;
            vlsHardwareIdentifier.Length,                        # 58 H  vls_t HardwareIdentifier;
            vlsClientName.Offset,                                # 62 H  vls_t ClientName;
            vlsClientName.Length,                                # 66 H  vls_t ClientName;
            self.MarketDataTransmissionInterval)                 # 70 i  int32_t MarketDataTransmissionInterval;



        msg += pack(vlsUsername.Format,             vlsUsername.Bytes)           
        msg += pack(vlsPassword.Format,             vlsPassword.Bytes)           
        msg += pack(vlsGeneralTextData.Format,      vlsGeneralTextData.Bytes)    
        msg += pack(vlsTradeAccount.Format,         vlsTradeAccount.Bytes)       
        msg += pack(vlsHardwareIdentifier.Format,   vlsHardwareIdentifier.Bytes) 
        msg += pack(vlsClientName.Format,           vlsClientName.Bytes)    
        
        self.ParseFromString(msg)
        return msg

    def ParseFromString(self, msg):
        vlsUsername  = s_vlsf()
        vlsPassword = s_vlsf()
        vlsGeneralTextData = s_vlsf()
        vlsTradeAccount = s_vlsf()
        vlsHardwareIdentifier = s_vlsf()
        vlsClientName = s_vlsf()
        
        #padded_fmt = padder(self._fmt)
        fmt_padded = '<HxxiHHHHHHiiiiHHHHHHi'
        fmt_size = calcsize(fmt_padded)
        attrs = unpack(fmt_padded, msg[0:fmt_size])

        self.BaseSize = attrs[0]                     
        assert(self.BaseSize == fmt_size + 4)
        self.ProtocolVersion = attrs[1]              
        vlsUsername.Offset = attrs[2]                
        vlsUsername.Length = attrs[3]                
        vlsPassword.Offset = attrs[4]                
        vlsPassword.Length = attrs[5]                
        vlsGeneralTextData.Offset = attrs[6]         
        vlsGeneralTextData.Length = attrs[7]         
        self.Integer_1 = attrs[8]                    
        self.Integer_2 = attrs[9]                    
        self.HeartbeatIntervalInSeconds = attrs[10]   
        self.TradeModeEnum = attrs[11]                
        vlsTradeAccount.Offset = attrs[12]            
        vlsTradeAccount.Length = attrs[13]            
        vlsHardwareIdentifier.Offset = attrs[14]      
        vlsHardwareIdentifier.Length = attrs[15]      
        vlsClientName.Offset = attrs[16]              
        vlsClientName.Length = attrs[17]              
        self.MarketDataTransmissionInterval = attrs[18]

        self.Username  = GetVariableLengthStringField(msg, self.BaseSize, vlsUsername, 6)
        self.Password = GetVariableLengthStringField(msg, self.BaseSize, vlsPassword, 14)
        self.GeneralTextData = GetVariableLengthStringField(msg, self.BaseSize, vlsGeneralTextData, 22)
        self.TradeAccount = GetVariableLengthStringField(msg, self.BaseSize, vlsTradeAccount, 46)        # Fails here
        self.HardwareIdentifier = GetVariableLengthStringField(msg, self.BaseSize, vlsHardwareIdentifier, 10)
        self.ClientName = GetVariableLengthStringField(msg, self.BaseSize, vlsClientName, 10)

        return

class LogonResponse():
    _fmt = '<|H|i|i|HH|HH|i|HH|B|B|B|B|HH|B|B|B|B|B|B|B|B|B'
    BaseSize= None                                      # 0  H  uint16_t BaseSize;
    ProtocolVersion= None                               # 2  i  int32_t ProtocolVersion;
    LogonStatusEnum = None                              # 6  i  DTC::LogonStatusEnum Result;
    ResultText= None                                    # 10 HH vls_t ResultText;
    ReconnectAddress= None                              # 14 HH vls_t ReconnectAddress;
    Integer_1= None                                     # 18 i  int32_t Integer_1;
    ServerName= None                                    # 22 HH vls_t ServerName;
    MarketDepthUpdatesBestBidAndAsk= None               # 26 B  uint8_t MarketDepthUpdatesBestBidAndAsk;
    TradingIsSupported= None                            # 27 B  uint8_t TradingIsSupported;
    OCOOrdersSupported= None                            # 28 B  uint8_t OCOOrdersSupported;
    OrderCancelReplaceSupported= None                   # 29 B  uint8_t OrderCancelReplaceSupported;
    SymbolExchangeDelimiter= None                       # 30 HH vls_t SymbolExchangeDelimiter;
    SecurityDefinitionsSupported= None                  # 34 B  uint8_t SecurityDefinitionsSupported;
    HistoricalPriceDataSupported= None                  # 35 B  uint8_t HistoricalPriceDataSupported;
    ResubscribeWhenMarketDataFeedAvailable= None        # 36 B  uint8_t ResubscribeWhenMarketDataFeedAvailable;
    MarketDepthIsSupported= None                        # 37 B  uint8_t MarketDepthIsSupported;
    OneHistoricalPriceDataRequestPerConnection= None    # 38 B  uint8_t OneHistoricalPriceDataRequestPerConnection;
    BracketOrdersSupported= None                        # 39 B  uint8_t BracketOrdersSupported;
    Unused_1= None                                      # 40 B  uint8_t Unused_1; 
    UsesMultiplePositionsPerSymbolAndTradeAccount= None # 41 B  uint8_t UsesMultiplePositionsPerSymbolAndTradeAccount
    MarketDataSupported= None                           # 42 B  uint8_t MarketDataSupported;

    def ParseFromString(self, msg):
        vlsResultText= s_vlsf()
        vlsReconnectAddress= s_vlsf()
        vlsServerName= s_vlsf() 
        vlsSymbolExchangeDelimiter= s_vlsf()

                 
        fmt_padded = padder(self._fmt)  # fix the format string to match pragma pack(8)
        fmt_size = calcsize(fmt_padded)
        attrs = unpack(fmt_padded, msg[0:fmt_size])
    
        self.BaseSize = attrs[0]
        assert(self.BaseSize == fmt_size + 4)
        self.ProtocolVersion = attrs[1]
        self.Result  = attrs[2]
        vlsResultText.Offset = attrs[3]
        vlsResultText.Length = attrs[4]
        vlsReconnectAddress.Offset = attrs[5]
        vlsReconnectAddress.Length = attrs[6]
        self.Integer_1 = attrs[7]
        vlsServerName.Offset = attrs[8]
        vlsServerName.Length = attrs[9]
        self.MarketDepthUpdatesBestBidAndAsk = attrs[10]
        self.TradingIsSupported = attrs[11]
        self.OCOOrdersSupported = attrs[12]
        self.OrderCancelReplaceSupported = attrs[13]
        vlsSymbolExchangeDelimiter.Offset = attrs[14]
        vlsSymbolExchangeDelimiter.Length = attrs[15]
        self.SecurityDefinitionsSupported = attrs[16]
        self.HistoricalPriceDataSupported = attrs[17]
        self.ResubscribeWhenMarketDataFeedAvailable = attrs[18]
        self.MarketDepthIsSupported = attrs[19]
        self.OneHistoricalPriceDataRequestPerConnection = attrs[20]
        self.BracketOrdersSupported = attrs[21]
        #self.Unused_1 = attrs[22]
        self.UsesMultiplePositionsPerSymbolAndTradeAccount = attrs[22]
        self.MarketDataSupported = attrs[23]

        self.ResultText              = GetVariableLengthStringField(msg, self.BaseSize, vlsResultText, 10)
        self.ReconnectAddress        = GetVariableLengthStringField(msg, self.BaseSize, vlsReconnectAddress, 14)
        self.ServerName              = GetVariableLengthStringField(msg, self.BaseSize, vlsServerName, 22)
        self.SymbolExchangeDelimiter = GetVariableLengthStringField(msg, self.BaseSize, vlsSymbolExchangeDelimiter, 30)
        return


class HistoricalPriceDataRequest():
    _fmt = '<|H|i|H|H|H|H|i|q|q|I|B|B|H'  # fmt_padded = '<HxxiHHHHixxxxqqIBBH'    # This one works for the local server
                                        # fmt_padded = '<HxxixxxxHHHHiqqIBBH'    # This one works for remote !
    BaseSize = None                            # Hxx    0  uint16_t BaseSize;
    RequestID = None                           # i      4  int32_t RequestID;
    Symbol = None                              # HH     8  vls_t Symbol;
    Exchange  = None                           # HH     12 vls_t Exchange;
    RecordInterval = None                      # i      20 DTC::HistoricalDataIntervalEnum RecordInterval;
    StartDateTime = None                       # q      24 DTC::t_DateTime StartDateTime;
    EndDateTime = None                         # q      32 DTC::t_DateTime EndDateTime;
    MaxDaysToReturn = None                     # I      40 uint32_t MaxDaysToReturn;
    UseZLibCompression = 0                     # B      44 uint8_t UseZLibCompression;
    RequestDividendAdjustedStockData = 0       # B      45 uint8_t RequestDividendAdjustedStockData;
    Integer_1 = 0                              # H      46 uint16_t Integer_1;

    def SerializeToString(self,):

        fmt_padded = padder(self._fmt)
        fmt_padded = '<HxxixxxxHHHHiqqIBBH'
        self.BaseSize = calcsize(fmt_padded) + 4

        bss = self.BaseSize
        bss, vlsSymbol   = AddVariableLengthStringField(bss, self.Symbol)
        bss, vlsExchange = AddVariableLengthStringField(bss, self.Exchange)

        msg = pack(fmt_padded, 
                    self.BaseSize,
                    self.RequestID,
                    vlsSymbol.Offset,
                    vlsSymbol.Length,
                    vlsExchange.Offset,
                    vlsExchange.Length,
                    self.RecordInterval,
                    self.StartDateTime,
                    self.EndDateTime,
                    self.MaxDaysToReturn,
                    self.UseZLibCompression,
                    self.RequestDividendAdjustedStockData,
                    self.Integer_1)

        msg += pack(vlsSymbol.Format, vlsSymbol.Bytes)
        msg += pack(vlsExchange.Format, vlsExchange.Bytes)
        #self.ParseFromString(msg)
        return msg

    def ParseFromString(self, msg):
        vlsSymbol= s_vlsf()
        vlsExchange = s_vlsf();

        fmt_padded = padder(self._fmt)  # fix the format string to match pragma pack(8)
        fmt_padded = '<HxxixxxxHHHHiqqIBBHxxxxxx'
        fmt_size = calcsize(fmt_padded)

        attrs = unpack(fmt_padded, msg[0:fmt_size])
        self.BaseSize = attrs[0]
        assert(self.BaseSize == fmt_size + 4)
        self.RequestID = attrs[1]
        vlsSymbol.Offset = attrs[2]
        vlsSymbol.Length = attrs[3]
        vlsExchange.Offset = attrs[4]
        vlsExchange.Length = attrs[5]
        self.RecordInterval = attrs[6]
        self.StartDateTime = attrs[7]
        self.EndDateTime = attrs[8]
        self.MaxDaysToReturn = attrs[9]
        self.UseZLibCompression = attrs[10]
        self.RequestDividendAdjustedStockData = attrs[11]
        self.Integer_1 = attrs[12]

        self.Symbol = GetVariableLengthStringField(msg, self.BaseSize, vlsSymbol, 8)
        self.Exchange = GetVariableLengthStringField(msg, self.BaseSize, vlsExchange, 12)
        return

class HistoricalPriceDataReject():
    _fmt = '<|H|i|H|H'
    BaseSize = None      # H   0   uint16_t BaseSize;
    RequestID = None     # i   2   int32_t RequestID
    RejectText = None    # Hxx 8   vls_t RejectText;

    def ParseFromString(self, msg):
        vlsRejectText= s_vlsf()

        fmt_padded = padder(self._fmt)  # fix the format string to match pragma pack(8)
        fmt_size = calcsize(fmt_padded)
        attrs = unpack(fmt_padded, msg[0:fmt_size])
    
        self.BaseSize = attrs[0]
        self.RequestID  = attrs[1]
        vlsRejectText.Offset = attrs[2]
        vlsRejectText.Length = attrs[3]

        self.RejectText = GetVariableLengthStringField(msg, self.BaseSize, vlsRejectText, 8)
        return


class GeneralLogMessage():
    _fmt = '<|H|H|H'
    BaseSize = None      # Hxx  0   uint16_t BaseSize;
    MessageText = None   # HH   4   vls_t RejectText;

    def ParseFromString(self, msg):
        vlsMessageText= s_vlsf()

        fmt_padded = padder(self._fmt)  # fix the format string to match pragma pack(8)
        fmt_size = calcsize(fmt_padded)
        attrs = unpack(fmt_padded, msg[0:fmt_size])
    
        self.BaseSize = attrs[0]
        vlsMessageText.Offset = attrs[1]
        vlsMessageText.Length = attrs[2]

        self.MessageText= GetVariableLengthStringField(msg, self.BaseSize, vlsMessageText, 4)
        return

# class HistoricalPriceDataResponseHeader() # see DTCProtocol_binary.py

class Logoff():
    _fmt = 'H|H|H|B'
    BaseSize = None         #      uint16_t BaseSize;
    Reason = None           #      vls_t Reason;
    DoNotReconnect = 0      #      uint8_t DoNotReconnect
    
    def SerializeToString():
        padded_fmt = padder(self._fmt)
        self.BaseSize = calcsize(padded_fmt) + 4

        bss = self.BaseSize
        bss, vlsReason   = AddVariableLengthStringField(bss, self.Reason)

        msg = pack(padded_fmt, 
                    self.BaseSize,
                    self.Reason.Offset,
                    self.Reason.Length,
                    self.DoNotReconnect)

        msg += pack(vlsReason.Format, vlsReason.Bytes)
        return msg

# Mapping message type to DTC message object and human readable name
DTC_MTYPE_MAP = {
    ENCODING_REQUEST: (EncodingRequest, "encoding request"),
    ENCODING_RESPONSE: (EncodingResponse, "encoding response"),
    LOGON_REQUEST: (LogonRequest, "logon request"),
    LOGON_RESPONSE: (LogonResponse, "logon response"),
#    HEARTBEAT: (Heartbeat, "heartbeat"),
    HISTORICAL_PRICE_DATA_REQUEST: (HistoricalPriceDataRequest, "historical data request"),
    HISTORICAL_PRICE_DATA_REJECT: (HistoricalPriceDataReject, "historical data reject"),
    HISTORICAL_PRICE_DATA_RESPONSE_HEADER: (HistoricalPriceDataResponseHeader,"historical data response header"),
    HISTORICAL_PRICE_DATA_RECORD_RESPONSE: (HistoricalPriceDataRecordResponse,"historical data record response"),
#    LOGOFF: (Logoff, "logoff"),
    GENERAL_LOG_MESSAGE: (GeneralLogMessage, "general log message")
}

def DtcMsgObjectFromType(msg_type):
    entry = DTC_MTYPE_MAP[msg_type]
    return entry[0]()

def DtcMsgNameFromType(msg_type):
    return DTC_MTYPE_MAP[msg_type][1]

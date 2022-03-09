from struct import pack, unpack, calcsize
from pysc.DTC.DTCProtocol_Enums import *
# ENUMS

def padder(fmt):
    #  pragma pack(n) The alignment of a member is on a boundary that's either a multiple of n, or a multiple of the size of the member, whichever is smaller.
    assert fmt[0:2] == '<|'
    padded_fmt = fmt[0] # should always be '<'
    pack_val = 8
    cnt = 0
    for f in fmt[1:].split('|')[1:]:
        size = calcsize(f)
        alignment = min(pack_val, size) # *whichever is smaller*
        new = (cnt + size)  // alignment
        old = cnt // alignment
        if new  > old:
            padding = ( cnt+size ) % alignment
            padded_fmt += padding * 'x'
            cnt += padding
        cnt += size
        padded_fmt += f
    padded_fmt += 'x' * (4 - calcsize(padded_fmt)%4)
    return padded_fmt

class EncodingRequest():
    _fmt = '<|i|i|4s'
    ProtocolVersion = None
    Encoding = None
    ProtocolType = None
    
    def SerializeToString(self,):
        assert(len(self.ProtocolType) <= 3)

        fmt_padded = padder(self._fmt)
        fmt_padded = self._fmt.replace('|','')
        msg = pack(fmt_padded,
                   self.ProtocolVersion,
                   self.Encoding,
                   (self.ProtocolType+'\0').encode('UTF-8') )        
        return msg


class EncodingResponse():
    ProtocolVersion = None
    Encoding = None
    ProtocolType = None

    def ParseFromString(self, msg):
		#int32_t ProtocolVersion;
		#EncodingEnum Encoding;
		#char ProtocolType[4];
        self.ProtocolVersion, self.Encoding, self.ProtocolType = unpack('<ii4s', msg)
        return

class HistoricalPriceDataResponseHeader():
    _fmt = '<|i|i|B|B|f'
    RequestID = None                    #      int32_t RequestID;
    RecordInterval= None                #      HistoricalDataIntervalEnum RecordInterval;
    UseZLibCompression= None            #      uint8_t UseZLibCompression;
    NoRecordsToReturn = None            #      uint8_t NoRecordsToReturn;
    IntToFloatPriceDivisor= None        #      float IntToFloatPriceDivisor;

    def ParseFromString(self, msg):
        fmt_padded = padder(self._fmt)  # fix the format string to match pragma pack(8)
        fmt_padded = '<iiBBxxf'
        fmt_size = calcsize(fmt_padded)
        attrs = unpack(fmt_padded, msg[0:fmt_size])
    
        self.RequestID  = attrs[0]
        self.RecordInterval = attrs[1]
        self.UseZLibCompression = attrs[2]
        self.NoRecordsToReturn  = attrs[3]
        self.IntToFloatPriceDivisor = attrs[4]
        return

class HistoricalPriceDataRecordResponse():
    _fmt = '<|i|q|d|d|d|d|d|Ixxxx|d|d|B'
    RequestID = None        #      int32_t RequestID;
    StartDateTime = None    #      t_DateTimeWithMicrosecondsInt StartDateTime; //Format can also be t_DateTime. Check value to determine
    OpenPrice = None        #      double OpenPrice;
    HighPrice = None        #      double HighPrice;
    LowPrice = None         #      double LowPrice;
    LastPrice = None        #      double LastPrice;
    Volume = None           #      double Volume;
    OpenInterest  = None    #      uint32_t OpenInterest /	uint32_t NumTrades;
    NumTrades = None
    BidVolume = None        #      double BidVolume;
    AskVolume = None        #      double AskVolume;
    IsFinalRecord = None    #      uint8_t IsFinalRecord;


    def ParseFromString(self, msg):
        fmt_padded = padder(self._fmt)  # fix the format string to match pragma pack(8)
        fmt_padded = self._fmt.replace('|','')
        fmt_size = calcsize(fmt_padded)
        attrs = unpack(fmt_padded, msg[0:fmt_size])

        self.RequestID = attrs[0]
        self.StartDateTime = attrs[1]
        self.OpenPrice = attrs[2]
        self.HighPrice = attrs[3]
        self.LowPrice = attrs[4]
        self.LastPrice = attrs[5]
        self.Volume = attrs[6]
        self.OpenInterest  = attrs[7]
        self.NumTrades  = attrs[7]
        self.BidVolume = attrs[8]
        self.AskVolume = attrs[9]
        self.IsFinalRecord = attrs[10]
        return

# Mapping message type to DTC message object and human readable name
DTC_MTYPE_MAP = {
    ENCODING_REQUEST: (EncodingRequest, "encoding request"),
    ENCODING_RESPONSE: (EncodingResponse, "encoding response"),
#    LOGON_REQUEST: (LogonRequest, "logon request"),
#    LOGON_RESPONSE: (LogonResponse, "logon response"),
#    HEARTBEAT: (Heartbeat, "heartbeat"),
#    HISTORICAL_PRICE_DATA_REQUEST: (HistoricalPriceDataRequest, "historical data request"),
#    HISTORICAL_PRICE_DATA_REJECT: (HistoricalPriceDataReject, "historical data reject"),
    HISTORICAL_PRICE_DATA_RESPONSE_HEADER: (HistoricalPriceDataResponseHeader,"historical data response header"),
    HISTORICAL_PRICE_DATA_RECORD_RESPONSE: (HistoricalPriceDataRecordResponse,"historical data record response"),
#    LOGOFF: (Logoff, "logoff"),
#    GENERAL_LOG_MESSAGE: (GeneralLogMessage, "general log message")
}

def DtcMsgObjectFromType(msg_type):
    entry = DTC_MTYPE_MAP[msg_type]
    return entry[0]()

def DtcMsgNameFromType(msg_type):
    return DTC_MTYPE_MAP[msg_type][1]

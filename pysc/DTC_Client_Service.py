from ctypes import WinError
from pysc.DTC import DTCProtocol_protobuf as DTC_protobuf
from pysc.DTC import DTCProtocol_binary as DTC_binary
from pysc.DTC import DTCProtocol_binaryVLS as DTC_binaryVLS
from google.protobuf.json_format import MessageToDict
import datetime as dt
import socket
import threading
import struct
import selectors
import collections
from threading import Thread
from queue import Queue
import time
import logging
import pandas as pd

logger = logging.getLogger(__name__)

class ApiNotAuthorised(Exception):
    '''Raised when the API rejects a historical data request becuase the user is not authorised'''
    pass

class ApiNoHistoricalData(Exception):
    '''Raised when the API rejects a historical data request becuase the user is not authorised'''
    pass

class ApiUnexpectedError(Exception):
    '''Raised when there is an error we really did't expect'''
    pass

class ApiInvalidSymbol(Exception):
    '''Raised when the API rejects a historical data request becuase the user is not authorised'''
    pass


class ApiResponse:
    def __init__(self, type, msg):
        self.type = type
        self.msg = msg


class DtcClientService():
    """ DTC connection and data retrieval

    """

    def __init__(self, dtc_server, encoding='protobuf', heartbeat=True, ClientName='', GeneralTextData='', HardwareIdentifier='', UserName=''):
        """ 
        """
        
        self.heartbeat = heartbeat
        self.ClientName = ClientName
        self.GeneralTextData = GeneralTextData
        self.HardwareIdentifier = HardwareIdentifier
        self.UserName = UserName
        self.dtc_server = dtc_server
        self.heartbeat_interval = 20
        self.messages = Queue()
        self.receiver_running = False
        self.logged_on = False
        
        self.request_id = 0  # Counter for ID used by HistoricalPriceDataRequest
        self.timeout = 30

        # Make the message library a class attribute so we can swap it easily
        if encoding=='protobuf':
            self.DTC = DTC_protobuf
            self.encoding_wanted = self.DTC.PROTOCOL_BUFFERS
        elif encoding=='binary':
            self.DTC = DTC_binary
            self.encoding_wanted = self.DTC.BINARY_ENCODING
        elif encoding=='binaryVLS':
            self.DTC = DTC_binaryVLS
            self.encoding_wanted = self.DTC.BINARY_WITH_VARIABLE_LENGTH_STRINGS
        else:
            raise RuntimeError('Encoding is invalid')

        return
        
    def login(self):
        self.logged_on = False

        while self.receiver_running:
            self.receiver_run = False
            self.sock.close()
            time.sleep(0.1)

        self.encoding = DTC_binary.BINARY_ENCODING  # Always start with binary encoding, switch after encoding request
        try:
            self.sock = socket.create_connection(self.dtc_server)
            self.selector = selectors.DefaultSelector()
            self.selector.register(self.sock, selectors.EVENT_READ)
        except ConnectionRefusedError:
            logger.exception(
                "Connection refused. Check that DTC server is running and listening at {}:{}".format(
                    dtc_server[0], dtc_server[1]
                ), exc_info=False)
            raise ConnectionError

        self.receiver_thread = receiver_thread(self, self.DTC)
        self.receiver_thread.start()

        logger.info(f'Requesting encoding: {self.encoding}')

        message = self.dtc_binary_encoding_request_message(self.encoding_wanted)
        
        self.send_message(message, 6, self.sock)

        response = self.messages.get(timeout=self.timeout)

        self.encoding = response.msg.Encoding
        logger.info(f'Agreed encoding: {self.encoding}')

        # Construct and send Logon Request
        logon_req = self.DTC.LogonRequest()
        logon_req.ProtocolVersion = self.DTC.CURRENT_VERSION
        logon_req.Username = self.UserName

        if self.heartbeat:
            logon_req.HeartbeatIntervalInSeconds = self.heartbeat_interval
        else:
            logon_req.HeartbeatIntervalInSeconds = -1

        logon_req.ClientName = self.ClientName
        logon_req.GeneralTextData = self.GeneralTextData
        logon_req.HardwareIdentifier = self.HardwareIdentifier

        message = logon_req.SerializeToString()
        self.send_message(message, self.DTC.LOGON_REQUEST, self.sock)

        logon_response = self.messages.get(timeout=self.timeout)

        logger.info(logon_response.msg.ResultText)

        if logon_response.msg.Result != self.DTC.LOGON_SUCCESS:
            logger.error(logon_response.msg.ResultText)
            self.logout()
        else:
            self.logged_on = True

        if self.heartbeat:
            # Begin sending heartbeats
            self.heartbeat_thread = Thread(target=self.heartbeat_thread)
            self.heartbeat_thread.start()

        return

    def heartbeat_thread(self):
        message = self.DTC.Heartbeat().SerializeToString()
        next_heartbeat_time = time.time()
        while self.logged_on:
            if time.time() > next_heartbeat_time:
                try:
                    self.send_message(message, self.DTC.HEARTBEAT, self.sock)
                    next_heartbeat_time = time.time() + self.heartbeat_interval
                except OSError:
                    # The socket closed, exit becuase we will need to log back on
                    break
            time.sleep(0.1)

    def send_message(self, m, m_type, sock, no_retry = False):
        """ Prepend header to the message and send it to the specified socket
        """
        total_len = 4 + len(m)  # 2 bytes Size + 2 bytes Type
        header = struct.pack('<HH', total_len, m_type)  # Prepare 4-byte little-endian header

        try:
            sock.send(header + m)  # Send message
        except OSError as exc:
            # we got disconnected, happens when debugging, try again
            if no_retry==False:
                self.login()
                sock.send(header + m)  # Send message
            else:
                # if logoff fails, we dont actually care
                # Can happen after a big history download due 
                # to being slow handling a big DF
                if m_type != self.DTC.LOGOFF:
                    raise exc
        logger.debug(f"Sent {self.DTC.DtcMsgNameFromType(m_type)}")

    def dtc_binary_encoding_request_message(self, encoding_enum):
        msg = DTC_binary.EncodingRequest()
        msg.ProtocolVersion  = self.DTC.CURRENT_VERSION
        msg.Encoding = encoding_enum
        msg.ProtocolType = 'DTC'
        return msg.SerializeToString()

    def logout(self):
        """ Gracefully logoff and close the connection """
        logger.info("Disconnecting from DTC server")        
        logoff = self.DTC.Logoff()
        logoff.Reason = "Client terminating"
        message = logoff.SerializeToString()
        self.send_message(message, self.DTC.LOGOFF, self.sock, no_retry=True)
        self.logged_on = False
        #self.selector.unregister(self.sock)
        self.sock.close()
        return

    def historical_data_request(self, symbol, exchange, interval, start=0, end=0):
        """ Request historical data from the server
        """
        logger.info('Starting HistoricalPriceDataRequest')
        self.request_id += 1
        print(f'REQUEST ID {self.request_id}**********************************')
        if interval =='TICK':
            rec_interval = self.DTC.INTERVAL_TICK
        elif interval =='M':
            rec_interval = self.DTC.INTERVAL_1_MINUTE
        elif interval =='H':
            rec_interval = self.DTC.INTERVAL_1_HOUR
        elif interval =='D':
            rec_interval = self.DTC.INTERVAL_1_DAY
        else:
            raise RuntimeError('Interval must be one of TICK, M, H or D')

        if start is None:
            start=0

        if end is None:
            end = 0

        self.hist_rec_interval = rec_interval
        self.hist_start_tstamp = start
        data_req = self.DTC.HistoricalPriceDataRequest()
        data_req.RequestID = self.request_id
        data_req.Symbol = symbol
        data_req.Exchange = exchange
        data_req.RecordInterval = rec_interval
        data_req.StartDateTime = int(pd.Timestamp(start).timestamp())
        data_req.EndDateTime = int(pd.Timestamp(end).timestamp())
        data_req.MaxDaysToReturn = 0
        message = data_req.SerializeToString()

        self.send_message(message, self.DTC.HISTORICAL_PRICE_DATA_REQUEST, self.sock)

        complete = False
        df = pd.DataFrame(columns = ['Open', 'High', 'Low', 'Close', 'TotalVolume', 'NumTrades', 'BidVol', 'AskVol'])
        recs=0
        while not complete:
            response = self.messages.get(timeout=self.timeout)

            if response.msg.RequestID != self.request_id:
                logger.error('Unexpected error, RequestID in response does not match request')
                raise ApiUnexpectedError
            if response.type == self.DTC.HISTORICAL_PRICE_DATA_REJECT:
                # If we got reject - display reject text and logout
                logger.error(response.msg.RejectText)
                raise ApiNotAuthorised
            elif response.type == self.DTC.HISTORICAL_PRICE_DATA_RESPONSE_HEADER:
                if bool(response.msg.NoRecordsToReturn):
                    # If no records available - give warning and logout
                    logger.info("No historical data records available. Check symbol name.")
                    raise ApiNoHistoricalData
            elif response.type == self.DTC.HISTORICAL_PRICE_DATA_RECORD_RESPONSE:
                    msg = response.msg
                    time = dt.datetime.fromtimestamp(msg.StartDateTime)
                    if msg.StartDateTime > 0:
                        df.loc[time] = [msg.OpenPrice, msg.HighPrice, msg.LowPrice, msg.LastPrice, msg.Volume, msg.NumTrades, msg.BidVolume, msg.AskVolume]
                    recs+=1
                    #print(f'Rec {recs} final {msg.IsFinalRecord}')
                    if msg.IsFinalRecord > 0:
                        complete = True
            else:
                logger.error('Received unexpected message waiting for response')
                raise ApiUnexpectedError

        logger.info(f'Received {recs} records and created DataFrame with {len(df)} records')
        return df

    def securityDefinitionForSymbol(self, symbol, exchange):
        """
        """
        assert self.messages.qsize() ==0
        logger.info(f"Starting SecurityDefinitionForSymbolRequest Symbol:'{symbol}' Exchange:'{exchange}'")
        self.request_id += 1
        print(f'REQUEST ID {self.request_id}**********************************\n')
        request = self.DTC.SecurityDefinitionForSymbolRequest()
        request.RequestID = self.request_id
        request.Symbol = symbol
        request.Exchange = exchange
        message = request.SerializeToString()
        self.send_message(message, self.DTC.SECURITY_DEFINITION_FOR_SYMBOL_REQUEST , self.sock)

        complete = False
        result = []
        while not complete:
            response = self.messages.get(timeout=self.timeout)

            if response.type == self.DTC.SECURITY_DEFINITION_REJECT :
                logger.error(response.msg.RejectText)
                raise ApiUnexpectedError
            elif response.type == self.DTC.SECURITY_DEFINITION_RESPONSE  :
                result.append(MessageToDict(response.msg))
                if response.msg.IsFinalMessage > 0:
                    complete = True
            else:
                logger.error('Received {self.DTC.DtcMsgNameFromType(m_type)}, was expecting {SecurityDefinitionForSymbolRequest}')
                raise ApiUnexpectedError

            if response.msg.ExchangeSymbol == '':
                logger.error(f'Symbol {symbol} does not appear to have a security definition')
                raise ApiInvalidSymbol

            if response.msg.RequestID != self.request_id:
                logger.error(f'Unexpected error, RequestID {response.msg.RequestID} in response does not match RequestID {self.request_id} in request')
                print('')
                raise ApiUnexpectedError

        logger.info(f"Received security definition for Symbol:'{symbol}' Exchange:'{exchange}'")
        return result

    def securityTypes(self):
        """
        """
        st ={}
        st['unset'] = self.DTC.SECURITY_TYPE_UNSET
        st['futures'] = self.DTC.SECURITY_TYPE_FUTURE
        st['stock'] = self.DTC.SECURITY_TYPE_STOCK
        st['forex'] = self.DTC.SECURITY_TYPE_FOREX
        st['index'] = self.DTC.SECURITY_TYPE_INDEX
        st['futures_strategy'] = self.DTC.SECURITY_TYPE_FUTURES_STRATEGY
        st['futures_option'] = self.DTC.SECURITY_TYPE_FUTURES_OPTION
        st['stock_option'] = self.DTC.SECURITY_TYPE_STOCK_OPTION
        st['index_option'] = self.DTC.SECURITY_TYPE_INDEX_OPTION
        st['bond'] = self.DTC.SECURITY_TYPE_BOND
        st['mutual_fund'] = self.DTC.SECURITY_TYPE_MUTUAL_FUND
        return st

    def SymbolsForUnderlyingRequest(self, symbol, exchange, securityType='futures'):
        """
        """
        logger.info('Starting SymbolsForUnderlyingRequest')
        SECURITY_TYPE = self.securityTypes()[securityType.lower()]
        self.request_id += 1
        request = self.DTC.SymbolsForUnderlyingRequest()
        request.RequestID = self.request_id
        request.UnderlyingSymbol = symbol
        request.Exchange = exchange
        request.SecurityType = SECURITY_TYPE
        message = request.SerializeToString()
        self.send_message(message, self.DTC.SYMBOLS_FOR_UNDERLYING_REQUEST , self.sock)

        complete = False
        result = []

        while not complete:
            response = self.messages.get(timeout=self.timeout)

            if response.msg.RequestID != self.request_id:
                logger.error('Unexpected error, RequestID in response does not match request')
                raise ApiUnexpectedError

            if response.type == self.DTC.SECURITY_DEFINITION_REJECT :
                logger.error(response.msg.RejectText)
                raise ApiUnexpectedError
            elif response.type == self.DTC.SECURITY_DEFINITION_RESPONSE  :
                underlying = MessageToDict(response.msg)
                print(underlying)
                result.append(underlying)
                if response.msg.IsFinalMessage > 0:
                    complete = True
            else:
                logger.error(response.msg.RejectText)
                raise ApiUnexpectedError

        logger.info(f'Received {len(result)} security definitions for synbol:"{symbol}" exchange:"{exchange}"')
        return result

    def SymbolSearchRequest(self, searchText, exchange, securityType='futures', searchType = 'by_symbol'):
        """
        """
        logger.info('Starting SymbolSearchRequest')
        SEARCH_TYPE_DICT={}
        SEARCH_TYPE_DICT['by_symbol'] = self.DTC.SEARCH_TYPE_BY_SYMBOL
        SEARCH_TYPE_DICT['by_description'] = self.DTC.SEARCH_TYPE_BY_DESCRIPTION
        SEARCH_TYPE = SEARCH_TYPE_DICT[searchType]
        SECURITY_TYPE = self.securityTypes()[securityType.lower()]

        self.request_id += 1
        request = self.DTC.SymbolSearchRequest()
        request.RequestID = self.request_id
        request.SearchText = searchText
        request.Exchange = exchange
        request.SecurityType = SECURITY_TYPE
        request.SearchType = SEARCH_TYPE
        message = request.SerializeToString()
        self.send_message(message, self.DTC.SYMBOL_SEARCH_REQUEST , self.sock)


        complete = False
        result = []
        while not complete:
            response = self.messages.get(timeout=self.timeout)

            if response.msg.RequestID != self.request_id:
                logger.error('Unexpected error, RequestID in response does not match request')
                raise ApiUnexpectedError

            if response.type == self.DTC.SECURITY_DEFINITION_REJECT :
                logger.error(response.msg.RejectText)
                raise ApiUnexpectedError
            elif response.type == self.DTC.SECURITY_DEFINITION_RESPONSE  :
                underlying = MessageToDict(response.msg)
                result.append(underlying)
                if response.msg.IsFinalMessage > 0:
                    complete = True
            else:
                logger.error(response.msg.RejectText)
                raise ApiUnexpectedError

        logger.info(f'Received {len(result)} security definitions for synbol:"{symbol}" exchange:"{exchange}"')
        return result

    def UnderlyingSymbolsForExchangeRequest(self, exchange, securityType='futures'):
        """
        """
        logger.info('Starting SymbolSearchRequest')
        SECURITY_TYPE = self.securityTypes()[securityType.lower()]

        self.request_id += 1
        request = self.DTC.UnderlyingSymbolsForExchangeRequest()
        request.RequestID = self.request_id
        request.Exchange = exchange
        request.SecurityType = SECURITY_TYPE
        message = request.SerializeToString()
        self.send_message(message, self.DTC.UNDERLYING_SYMBOLS_FOR_EXCHANGE_REQUEST  , self.sock)

        complete = False
        result = []
        while not complete:
            response = self.messages.get(timeout=self.timeout)

            if response.msg.RequestID != self.request_id:
                logger.error('Unexpected error, RequestID in response does not match request')
                raise ApiUnexpectedError

            if response.type == self.DTC.SECURITY_DEFINITION_REJECT :
                logger.error(response.msg.RejectText)
                raise ApiUnexpectedError
            elif response.type == self.DTC.SECURITY_DEFINITION_RESPONSE  :
                underlying = MessageToDict(response.msg)
                result.append(underlying)
                if response.msg.IsFinalMessage > 0:
                    complete = True
            else:
                logger.error(response.msg.RejectText)
                raise ApiUnexpectedError

        logger.info(f'Received {len(result)} security definitions for exchange:"{exchange}"')
        return result

class receiver_thread(Thread):
    """
    """
    def __init__(self, parent, DTC):
        """
        """
        self.parent = parent
        self.DTC = DTC
        super().__init__()

    def run(self):
        """ 
        """
        logger.info(f'Message receiver thread starting')
        self.parent.receiver_run = True
        timeout_secs = 30
        self.parent.error = None
        while self.parent.receiver_run:
            self.parent.receiver_running = True
            events = self.parent.selector.select()#timeout=timeout_secs)
            # Either we received data or the socket closed

            try:
                msg_header = self.parent.sock.recv(4)
            except (OSError, WindowsError) as exc:
                # The socket was closed
                # Happens if we call logout on the DTC server
                # Exit the thread loop
                logger.info(f'Socket Closed, receiver thread exiting')
                break

            if msg_header == b'':
                self.parent.error = 'timeout'
                break

            msg_size = struct.unpack_from('<H', msg_header)[0]
            msg_type = struct.unpack_from('<H', msg_header, 2)[0]

            msg_body = self.parent.sock.recv(msg_size - 4)

            #logger.info(f'Received {msg_type} message')

            try:
                if self.parent.encoding == self.parent.DTC.BINARY_ENCODING:
                    dtc_message = DTC_binary.DtcMsgObjectFromType(msg_type);
                elif self.parent.encoding == self.parent.DTC.PROTOCOL_BUFFERS:
                    dtc_message = DTC_protobuf.DtcMsgObjectFromType(msg_type);
                elif self.parent.encoding == self.parent.DTC.BINARY_WITH_VARIABLE_LENGTH_STRINGS:
                    dtc_message = DTC_binaryVLS.DtcMsgObjectFromType(msg_type);
                else:
                    raise RuntimeError('Invalid encoding type')
            except KeyError:

                logger.info("Received unknown message type: {0}".format(msg_type))
                self.parent.error = 'key error'
                break

            dtc_message.ParseFromString(msg_body)
  
            if msg_type == self.parent.DTC.GENERAL_LOG_MESSAGE:
                logger.info("Got general message from server: '{}'".format(dtc_message.MessageText))
                continue
            elif msg_type == self.parent.DTC.HEARTBEAT:
                logger.debug("Recv {0}".format(self.DTC.DtcMsgNameFromType(msg_type)))
                continue # Do not store heartbeat messages

            self.parent.messages.put( ApiResponse(msg_type,dtc_message) )
        self.parent.receiver_running = False
        return
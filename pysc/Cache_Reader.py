import os
import numpy as np
import pandas as pd
from glob import glob
import datetime as dt

def sc_cached_contracts_data_for_symbol(cache_location, symbol, frequency:str):
    '''
    Search for contracts. You can use * and ? as wildcards
    '''
    if frequency not in ['D', 'H', 'M']:
        raise RuntimeError('Requested time frame not one of H, M, D')

    if not os.path.exists(cache_location):
        raise RuntimeError(f'Cache path {cache_location} does not exist')

    if frequency =='D':
        search_path = os.path.join(cache_location, symbol + '*.dly')
    else:
        search_path = os.path.join(cache_location, symbol + '*.scid')

    files = glob(search_path)

    contracts = [os.path.split(f)[1].split('.')[0] for f in files]

    return contracts

def sc_cached_price_data_for_symbol(cache_location:str, symbol:str, frequency:str, start:dt.datetime=None, end:dt.datetime=None):
    if frequency not in ['D', 'H', 'M']:
        raise RuntimeError('Requested time frame not one of H, M, D')

    if frequency =='D':
        filepath = os.path.join(cache_location, symbol + '.dly')
        df = dly_to_df(filepath)
    else:
        filepath = os.path.join(cache_location, symbol + '.scid')
        df = scid_to_df(filepath)

    if df.empty:
        return df

    if start is None:
        start = df.index[0]
    else:
        start = pd.Timestamp(start)

    if end is None:
        end = df.index[-1]
    else:
        end = pd.Timestamp(end)

    date_mask = (df.index >= start) & (df.index <= end)
    df = df.loc[date_mask]  # slice the data to the interval we want

    if frequency == 'H':
        df = df.resample('1H', label='right', closed='right').agg({'Open':'first',
                                                              'High':'max',
                                                              'Low':'min',
                                                              'Close':'last',
                                                              'NumTrades':'sum',
                                                              'TotalVolume':'sum',
                                                              'BidVolume':'sum',
                                                              'AskVolume':'sum'})
    elif frequency == 'M':
        df = df.resample('1T', label='right', closed='right').agg({'Open':'first',
                                                              'High':'max',
                                                              'Low':'min',
                                                              'Close':'last',
                                                              'NumTrades':'sum',
                                                              'TotalVolume':'sum',
                                                              'BidVolume':'sum',
                                                              'AskVolume':'sum'})

    elif frequency == 'D':
        pass
        # No resample, we opened a dly file, which has the correct sampling already
    else:
        # Should NEVER get here
        raise RuntimeError('Unexpected sampling interval')
    
    df = df.dropna() # bin empty rows

    return df

def scid_to_df(filepath):
    names = 'DateTime','Open','High','Low','Close','NumTrades','TotalVolume','BidVolume','AskVolume'
    offsets = 0, 8, 12, 16, 20, 24, 28, 32, 36
    formats = 'u8', 'f4', 'f4', 'f4', 'f4', 'i4', 'i4', 'i4', 'i4'
    np_format = np.dtype({'names':names, 'offsets':offsets, 'formats':formats}, align=True)

    file = open(filepath, 'rb')
    filetype    = file.read(4)
    headersize  = np.fromfile(file, 'i4', count=1)[0]
    recordSize  = np.fromfile(file, 'i4', count=1)[0]
    version     = np.fromfile(file, 'i2', count=1)[0]

    file.seek(headersize, os.SEEK_SET)

    df = pd.DataFrame(np.fromfile(file, np_format))
    if not df.empty:
        df.index = pd.to_datetime(df['DateTime'], origin='1899-12-30', unit='us')
        #df.index = pd.to_pydatetime(df['DateTime'], origin='1899-12-30', unit='us')
        del df['DateTime']

    return df

def dly_to_df(filepath):
    df = pd.read_csv(filepath, index_col = 0)
    df.index = pd.to_datetime(df.index)

    return df
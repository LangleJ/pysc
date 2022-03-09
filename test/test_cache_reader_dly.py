import pytest
from pysc.Cache_Reader import dly_to_df, sc_cached_price_data_for_symbol
import datetime as dt
import os

cache_loc = 'G:\\Sierra'
test_symbol = 'BRN'
test_contract = 'BRNZ21-ICEEU'
test_dly_filepath = os.path.join(cache_loc, test_contract + '.dly')

class TestCacheReaderDly:

    def test_good_dly_whole_file(self):
        df = dly_to_df(test_dly_filepath)
        assert len(df) > 0

    def test_good_dly_between_dates(self):
        start = dt.datetime(2021, 1, 1)
        end = dt.datetime(2021, 6, 30)
        df = sc_cached_price_data_for_symbol(cache_loc, test_contract, 'H', start, end)
        assert df.index.min() >= start
        assert df.index.max() <= end


if __name__=="__main__":
    pytest.main(["-x", "test"])



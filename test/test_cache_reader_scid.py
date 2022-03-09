import pytest
from pysc.Cache_Reader import sc_cached_contracts_data_for_symbol
from pysc.Cache_Reader import sc_cached_price_data_for_symbol
from pysc.Cache_Reader import scid_to_df
from pysc.Cache_Reader import dly_to_df
import datetime as dt
import os

cache_loc = 'G:\\Sierra'
test_symbol = 'BRN'
test_contract = 'BRNZ21-ICEEU'
test_scid_filepath = os.path.join(cache_loc, test_contract + '.scid')

class TestCacheReaderScid:

    def test_good_scid_whole_file(self):
        df = scid_to_df(test_scid_filepath)
        assert len(df) > 0
        assert True

    def test_good_scid_between_dates(self):
        start = dt.datetime(2021, 1, 1)
        end = dt.datetime(2021, 6, 30)
        df = sc_cached_price_data_for_symbol(cache_loc, test_contract, 'H', start, end)
        assert df.index.min() >= start
        assert df.index.max() <= end


if __name__=="__main__":
    pytest.main(["-x", "test"])


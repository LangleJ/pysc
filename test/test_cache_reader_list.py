import pytest
from pysc.Cache_Reader import sc_cached_contracts_data_for_symbol

cache_loc = 'G:\\Sierra'
test_symbol = 'BRN'

class TestCacheReaderList:

    def test_list_intraday(self):
        contracts = sc_cached_contracts_data_for_symbol(cache_loc, test_symbol, 'H')
        assert len(contracts) > 0

    def test_list_daily(self):
        contracts = sc_cached_contracts_data_for_symbol(cache_loc, test_symbol, 'D')
        assert len(contracts) > 0

if __name__=="__main__":
    pytest.main(["-x", "test"])


if __name__=="__main__":
    pytest.main(["-x", "test"])


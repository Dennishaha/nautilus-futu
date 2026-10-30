from decimal import Decimal

import pytest
from nautilus_trader.model import BarType

from nautilus_futu.history import make_bar
from nautilus_futu.mapping import price
from tests.fakes import RECEIVED


@pytest.mark.parametrize("opening", [68.099999999, 110.024219264])
def test_historical_source_digits_survive_nt_conversion(opening):
    row = dict(
        code="US.IWM",
        time_key="2010-10-04 00:00:00",
        open=opening,
        high=112,
        low=60,
        close=100,
        volume=1000,
    )
    bar = make_bar(BarType.from_str("IWM.US.FUTU-1-DAY-LAST-EXTERNAL"), row, RECEIVED)
    assert bar.open.as_decimal() == Decimal(str(opening))
    other = make_bar(bar.bar_type, {**row, "open": 100}, RECEIVED)
    assert bar.open.precision == other.open.precision
    assert bar.ts_init == RECEIVED
    with pytest.raises(ValueError, match="refusing to round"):
        price(opening, 8)
    with pytest.raises(ValueError, match="invalid provider OHLC"):
        make_bar(bar.bar_type, {**row, "high": 90}, RECEIVED)

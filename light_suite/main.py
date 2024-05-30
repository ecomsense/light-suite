from master import download_masters, get_security_id
from login_get_broker import login
from constants import CRED
from streaming_indicators import CPR, SuperTrend, EMA
from traceback import print_exc
import pandas as pd
import sys

ind = {}
ind["cpr"] = CPR()
ind["long"] = EMA(period=6)
ind["short"] = EMA(period=1)
ind["st"] = SuperTrend(atr_length=1, factor=2.1)


def init():
    try:
        download_masters()
        user = CRED["user"]
        api = login(**user)

        return api
    except Exception as e:
        print(e)
        print_exc()
        sys.exit(1)


def get_candles(api):
    try:
        instrument_name = "TCS"
        exchange = "NSE_EQ"
        instrument_type = "EQUITY"
        """
        security_id = get_security_id(exchange, instrument_name, instrument_type)
        print("security_id", security_id)
        """
        kwargs = dict(
            symbol=instrument_name,
            exchange_segment=exchange,
            instrument_type=instrument_type,
            expiry_code=0,
            from_date="2024-04-01",
            to_date="2024-05-30",
        )
        resp = api.broker.historical_daily_data(**kwargs)["data"]
        df = pd.DataFrame(data=resp)
        df["date"] = pd.to_datetime(df["start_Time"], unit="s")
        df["date"] = df.date.dt.strftime("%Y-%m-%d")
        df = df.drop(columns=["start_Time"])
        print(df.tail())
        return df
    except Exception as e:
        print(f" get_candles {e}")
        print_exc()


def apply_indicators(row_as_dict):
    _cpr, _bc, _tc = ind["cpr"].update(row_as_dict)
    _ema_long = ind["long"].update(row_as_dict["close"])
    _ema_short = ind["short"].update(row_as_dict["close"])
    _st, _final_band = ind["st"].update(row_as_dict)
    return _cpr, _bc, _tc, _ema_long, _ema_short, _st, _final_band


def main():
    try:
        api = init()
        df = get_candles(api)
        df[["cpr", "bc", "tc", "ema_60m", "ema_15m", "ST Direction", "ST Value"]] = (
            df.apply(
                lambda row: apply_indicators(row.to_dict()),
                axis="columns",
                result_type="expand",
            )
        )
        print(df)

    except Exception as e:
        print(e)
        print_exc()


main()

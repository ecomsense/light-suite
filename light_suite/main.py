from master import download_masters, get_security_id
from login_get_broker import login
from constants import CRED
import traceback

try:
    user = CRED["user"]
    api = login(**user)
    print(api.margins)

    download_masters()
    exchange = "NSE"
    instrument_name = "Nifty Bank"
    instrument_type = "BANKNIFTY"

    security_id = get_security_id(exchange, instrument_name, instrument_type)
    if security_id is not None:
        print("security_id", security_id)
        kwargs = dict(symbol=exchange + ":" + security_id, type="INDEX")
        resp = api.intraday(**kwargs)
        print(resp)

    resp = api.broker.intraday_minute_data(
        security_id="11536", exchange_segment="NSE_EQ", instrument_type="EQUITY"
    )
    print(resp)


except Exception as e:
    print(e)
    print(traceback.format_exc())

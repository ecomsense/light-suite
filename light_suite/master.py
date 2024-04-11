import requests
import pandas as pd
from constants import CRED, DATA, FUTL
from typing import Dict
from datetime import datetime as dt


DUMP = DATA + "masters.csv"


def _filter_masters() -> None:
    df = pd.read_csv(DUMP)
    print(df.columns)
    vals = df[" SEM_SEGMENT"].unique()
    print(vals)
    columns = [
        "SEM_EXM_EXCH_ID",
        "SEM_SMST_SECURITY_ID",
        "SEM_CUSTOM_SYMBOL",
        "SEM_TRADING_SYMBOL",
        "SEM_LOT_UNITS",
        "SEM_TICK_SIZE",
        "SEM_INSTRUMENT_NAME",
    ]
    df = df[columns]
    exchanges = ["NSE", "BSE"]
    for exchange in exchanges:
        df_temp = df[df["SEM_EXM_EXCH_ID"] == exchange]
        print(df_temp.tail())
        df_temp.to_csv(DATA + exchange + ".csv", index=False)


def download_masters() -> None:
    if FUTL.is_file_not_2day(DUMP):
        print(f"Downloading {DUMP}..")
        r = requests.get(
            "https://images.dhan.co/api-data/api-scrip-master.csv")
        FUTL.write_file(DUMP, r.text)
        _filter_masters()


def _expand_expiry(day: str, month: str) -> str:
    mon_to_num = dict(
        JAN="01",
        FEB="02",
        MAR="03",
        APR="04",
        MAY="05",
        JUN="06",
        JUL="07",
        AUG="08",
        SEP="09",
        OCT="10",
        NOV="11",
        DEC="12",
    )
    return f"{day}-{mon_to_num[month]}-{dt.now().year}"


def _split_key_to_val(dct) -> Dict:
    updated_dct = {}
    for key, value in dct.items():
        parts = key.split(" ")
        if len(parts) == 5:
            nested_dct = {
                "expiry": _expand_expiry(day=parts[1], month=parts[2]),
                "strike": parts[3],
                "opt": "CE" if parts[4] == "CALL" else "PE",
                "security_id": value["SEM_SMST_SECURITY_ID"],
                "lot_size": value["SEM_LOT_UNITS"],
            }
            if parts[0] not in updated_dct:
                updated_dct[parts[0]] = [{**nested_dct}]
            else:
                updated_dct[parts[0]].append({**nested_dct})
    return updated_dct


def make_dict():
    exchanges = list(CRED["exchanges"].keys())
    lst = ["SEM_SMST_SECURITY_ID", "SEM_CUSTOM_SYMBOL", "SEM_LOT_UNITS"]
    for exchange in exchanges:
        dct_of_dct = {}
        df = pd.read_csv(DATA + exchange + ".csv", usecols=lst)
        df = df.dropna(subset=["SEM_CUSTOM_SYMBOL"])
        words_to_filter = CRED["exchanges"][exchange]
        filtered_df = df[
            df["SEM_CUSTOM_SYMBOL"].str.startswith(tuple(words_to_filter))
        ].set_index("SEM_CUSTOM_SYMBOL")
        dct = filtered_df.to_dict(orient="index")
        dct = _split_key_to_val(dct)
        dct_of_dct = {**dct_of_dct, **dct}
        yield dct_of_dct


def get_security_id(exchange, instrument_name, instrument_type):
    """Returns the SEM_SMST_SECURITY_ID based on search criteria.

    Args:
        exchange (str): csv file name prefix to read
        instrument_name (str): The SEM_INSTRUMENT_NAME to search for.
        instrument_type (str): The SEM_CUSTOM_SYMBOL to search for.

    Returns:
        str: The SEM_SMST_SECURITY_ID if a unique match is found, otherwise None.
    """

    df = pd.read_csv(DATA + exchange + ".csv")
    # Filter DataFrame based on search criteria
    filtered_df = df[
        (df["SEM_INSTRUMENT_NAME"] == instrument_name)
        & (df["SEM_CUSTOM_SYMBOL"] == instrument_type)
    ]

    # Check if exactly one row matches (ensuring unique match)
    if len(filtered_df) == 1:
        return filtered_df["SEM_SMST_SECURITY_ID"].values[
            0
        ]  # Return first element from Series
    else:
        return None  # No match or multiple matches found

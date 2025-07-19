import logging

import pandas as pd
import warnings
from quantcalendar import timestamp_us
from quantdata import get_conn_duckdb

warnings.simplefilter("default")

create_sql_stock_daily = """
dt TIMESTAMP_S PRIMARY KEY,
name VARCHAR,
_open FLOAT,
_high FLOAT,
_low FLOAT,
_close FLOAT,
volume UINTEGER,
amount UBIGINT,
preclose FLOAT,
net_profit_ttm FLOAT,
cashflow_ttm FLOAT,
equity FLOAT,
asset FLOAT,
debt FLOAT,
debttoasset FLOAT,
net_profit_q FLOAT,
pe_ttm FLOAT,
pb FLOAT,
mkt_cap DOUBLE,
mkt_cap_ashare DOUBLE,
vip_buy_amt FLOAT,
vip_sell_amt FLOAT,
inst_buy_amt FLOAT,
inst_sell_amt FLOAT,
mid_buy_amt FLOAT,
mid_sell_amt FLOAT,
indi_buy_amt FLOAT,
indi_sell_amt FLOAT,
master_net_flow_in FLOAT,
master2_net_flow_in FLOAT,
vip_net_flow_in FLOAT,
mid_net_flow_in FLOAT,
inst_net_flow_in FLOAT,
indi_net_flow_in FLOAT,
total_sell_amt FLOAT,
total_buy_amt FLOAT,
net_flow_in FLOAT,
turnover FLOAT,
free_shares UBIGINT,
total_shares UBIGINT,
maxupordown TINYINT,
maxupordown_at_open TINYINT,
lb_up_count UTINYINT,
lb_down_count UTINYINT,
close FLOAT,
open FLOAT,
high FLOAT,
low FLOAT
"""

create_sql_stock_bars_or_index = """
dt TIMESTAMP PRIMARY KEY,
open FLOAT,
high FLOAT,
low FLOAT,
close FLOAT,
volume UBIGINT,
amount UBIGINT
"""

create_sql_cb_daily = """
dt TIMESTAMP PRIMARY KEY,
open FLOAT,
high FLOAT,
low FLOAT,
close FLOAT,
volume UINTEGER,
amount UBIGINT,
preclose FLOAT,
change FLOAT,
pct_chg FLOAT,
convert_price FLOAT,
cb_value FLOAT,
cb_over_rate FLOAT,
remain_size UBIGINT,
bond_over_rate FLOAT,
bond_value FLOAT,
call_price FLOAT,
call_price_tax FLOAT,
is_call TINYINT,
call_type TINYINT
"""

create_sql_ths_index_daily = """
dt TIMESTAMP PRIMARY KEY,
open FLOAT,
high FLOAT,
low FLOAT,
close FLOAT,
avg_price FLOAT,
change FLOAT,
pct_change FLOAT,
volume UBIGINT,
turnover_rate FLOAT
"""

table_sqls = {
    "bars_index": create_sql_stock_bars_or_index,
    "bars_stock": create_sql_stock_bars_or_index,
    "bars_stock_daily": create_sql_stock_daily,
    "bars_ths_index_daily": create_sql_ths_index_daily,
    "bars_cb_daily": create_sql_cb_daily,
}


def get_tbname(tbname):
    tbname = tbname.replace(".", "_").lower()
    if tbname[0] >= "0" and tbname[0] <= "9":
        return "_" + tbname
    return tbname


def create_or_replace_table(df: pd.DataFrame, tablename, dbname):
    warnings.warn("`create_or_replace_table` is deprecated because write duckdb is very slow, use `` instead.", DeprecationWarning, stacklevel=2)
    if df is None or df.empty:
        return
    conn = get_conn_duckdb()
    conn.register("create_table_from_df", df)
    tbname = f"{dbname}.{get_tbname(tablename)}"
    # CREATE TABLE ... AS ... 不支持声明类型定义约束
    conn.execute(
        f"DROP TABLE IF EXISTS {tbname}; CREATE TABLE {tbname} ({table_sqls[dbname]}); INSERT INTO {tbname} SELECT * FROM create_table_from_df;"
    )
    conn.unregister("create_table_from_df")


def insert_multi_tables(df: pd.DataFrame, dbname):
    warnings.warn("`insert_multi_tables` is deprecated because write duckdb is very slow, use `save_multi_tables` instead.", DeprecationWarning, stacklevel=2)
    if df is None or df.empty:
        return
    if "tablename" not in df:
        raise ValueError("Multi insert tables has no tablename")

    tablenames = df["tablename"]
    df.drop(columns=["tablename"], inplace=True)

    field_names = list(df.columns)
    s = ",".join(field_names)
    q = ",".join(["?"] * len(field_names))
    conn = get_conn_duckdb()
    for tablename, row in zip(tablenames, df.itertuples()):
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {dbname}.{get_tbname(tablename)} ({table_sqls[dbname]});"
        )
        conn.execute(
            f"INSERT OR REPLACE INTO {dbname}.{get_tbname(tablename)} ({s}) VALUES ({q});",
            row[1:],  # skip Index
        )
    logging.info(f"批量写入DuckDB[{dbname}]")


def save_multi_tables(df: pd.DataFrame, parquet_file):
    if df is None or df.empty:
        return
    if "symbol" not in df:
        raise ValueError("Multi insert tables has no symbol")

    df.to_parquet(parquet_file)

    logging.info(f"写入parquet[{parquet_file}]")


def update(dt, values, tablename, dbname):
    warnings.warn("`update` is deprecated because write duckdb is very slow, use `` instead.", DeprecationWarning, stacklevel=2)
    conn = get_conn_duckdb()
    if "dt" in values:
        _microsec = timestamp_us(values["dt"])
        values["dt"] = f"make_timestamp({_microsec})"
    set_columns = ",".join([f"{col}={value}" for col, value in values.items()])
    dt_microsec = timestamp_us(dt)
    tbname = f"{dbname}.{get_tbname(tablename)}"
    conn.execute(
        f"UPDATE {tbname} SET {set_columns} WHERE dt=make_timestamp({dt_microsec});"
    )
    # logging.info(f"单行更新DuckDB[{tbname}] on date:{dt}")


def insert_one(row: tuple, tablename, dbname):
    warnings.warn("`insert_one` is deprecated because write duckdb is very slow, use `` instead.", DeprecationWarning, stacklevel=2)
    conn = get_conn_duckdb()
    q = ",".join(["?"] * len(row))
    tbname = f"{dbname}.{get_tbname(tablename)}"
    conn.execute(f"CREATE TABLE IF NOT EXISTS {tbname} ({table_sqls[dbname]});")
    conn.execute(f"INSERT OR REPLACE INTO {tbname} VALUES ({q});", row)
    # logging.info(f"单行插入DuckDB[{tbname}]")

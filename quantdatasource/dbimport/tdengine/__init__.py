import logging
from functools import partial

import pandas as pd
import taos
from quantdata import get_conn_tdengine

table_types = {
    "ticks": {
        "dt": "timestamp",
        "last_price": "float",
        "volume": "int unsigned",
        "amount": "int unsigned",
        "bid_price1": "float",
        "ask_price1": "float",
        "bid_volume1": "int unsigned",
        "ask_volume1": "int unsigned",
        "open_interest": "int unsigned",
    },
    "tick_bars": {
        "dt": "timestamp",
        "open": "float",
        "high": "float",
        "low": "float",
        "close": "float",
        "volume": "int unsigned",
        "amount": "bigint unsigned",
        "open_interest": "int unsigned",
    },
    "bars_ctpfuture_daily": {
        "dt": "timestamp",
        "open": "float",
        "high": "float",
        "low": "float",
        "close": "float",
        "pre_close": "float",
        "pre_settle": "float",
        "settle": "float",
        "volume": "int unsigned",
        "amount": "bigint unsigned",
        "open_interest": "int unsigned",
    },
    "bars_stock_daily": {
        "dt": "timestamp",
        "name": "nchar",
        "open": "float",
        "high": "float",
        "low": "float",
        "close": "float",
        "volume": "int unsigned",
        "amount": "bigint unsigned",
        "preclose": "float",
        "net_profit_ttm": "float",
        "cashflow_ttm": "float",
        "equity": "float",
        "asset": "float",
        "debt": "float",
        "debttoasset": "float",
        "net_profit_q": "float",
        "pe_ttm": "float",
        "pb": "float",
        "mkt_cap": "double",
        "mkt_cap_ashare": "double",
        "vip_buy_amt": "float",
        "vip_sell_amt": "float",
        "inst_buy_amt": "float",
        "inst_sell_amt": "float",
        "mid_buy_amt": "float",
        "mid_sell_amt": "float",
        "indi_buy_amt": "float",
        "indi_sell_amt": "float",
        "master_net_flow_in": "float",
        "master2_net_flow_in": "float",
        "vip_net_flow_in": "float",
        "mid_net_flow_in": "float",
        "inst_net_flow_in": "float",
        "indi_net_flow_in": "float",
        "total_sell_amt": "float",
        "total_buy_amt": "float",
        "net_flow_in": "float",
        "turnover": "float",
        "free_shares": "bigint unsigned",
        "total_shares": "bigint unsigned",
        "maxupordown": "tinyint",
        "maxupordown_at_open": "tinyint",
        "lb_up_count": "tinyint unsigned",
        "lb_down_count": "tinyint unsigned",
        "close_": "float",
        "open_": "float",
        "high_": "float",
        "low_": "float",
    },
    "bars": {
        "dt": "timestamp",
        "open": "float",
        "high": "float",
        "low": "float",
        "close": "float",
        "volume": "bigint unsigned",
        "amount": "double",
    },
    "bars_ths_index_daily": {
        "dt": "timestamp",
        "open": "float",
        "high": "float",
        "low": "float",
        "close": "float",
        "avg_price": "float",
        "change": "float",
        "pct_change": "float",
        "volume": "bigint unsigned",
        "turnover_rate": "float",
    },
    "market_stats": {
        "dt": "timestamp",
        "count_of_uplimit": "smallint unsigned",
        "count_of_downlimit": "smallint unsigned",
        "count_of_yiziup": "smallint unsigned",
        "count_of_yizidown": "smallint unsigned",
        "ratio_of_uplimit": "float",
        "ratio_of_downlimit": "float",
        "ratio_of_yiziup": "float",
        "ratio_of_yizidown": "float",
        "lb1": "smallint unsigned",
        "lb2": "smallint unsigned",
        "lb3": "smallint unsigned",
        "lb4": "smallint unsigned",
        "lb5": "smallint unsigned",
        "lb6": "smallint unsigned",
        "lb7": "smallint unsigned",
        "lb8": "smallint unsigned",
        "lb9": "smallint unsigned",
        "lb10": "smallint unsigned",
        "lb11": "smallint unsigned",
        "lb12": "smallint unsigned",
    },
    "dpjk": {
        "dt": "timestamp",
        "sh000016": "float",
        "sh000905": "float",
        "sh000300": "float",
        "sh000852": "float",
        "sz399303": "float",
        "sh000001": "float",
        "sz399001": "float",
        "sz399006": "float",
    },
    "bars_cb_daily": {
        "dt": "timestamp",
        "open": "float",
        "high": "float",
        "low": "float",
        "close": "float",
        "volume": "int unsigned",
        "amount": "bigint unsigned",
        "preclose": "float",
        "change": "float",
        "pct_chg": "float",
        "convert_price": "float",
        "cb_value": "float",
        "cb_over_rate": "float",
        # 'accrued_interest': 'float',
        "remain_size": "bigint unsigned",
        "bond_over_rate": "float",
        "bond_value": "float",
        "call_price": "float",
        "call_price_tax": "float",
        "is_call": "tinyint",
        "call_type": "tinyint",
    },
}

param_funcs = {
    "float": taos.TaosMultiBind.float,
    "int unsigned": taos.TaosMultiBind.int_unsigned,
    "bigint unsigned": taos.TaosMultiBind.bigint_unsigned,
    "timestamp": taos.TaosMultiBind.timestamp,
    "binary": taos.TaosMultiBind.binary,
    "nchar": taos.TaosMultiBind.nchar,
    "bool": taos.TaosMultiBind.bool,
    "tinyint": taos.TaosMultiBind.tinyint,
    "double": taos.TaosMultiBind.double,
    "tinyint unsigned": taos.TaosMultiBind.tinyint_unsigned,
    "smallint unsigned": taos.TaosMultiBind.smallint_unsigned,
}


def get_tbname(tablename, stable=None):
    if stable is not None:
        return f"{stable}_{tablename.replace('.', '_')}".lower()
    else:
        return tablename.replace(".", "_").lower()


def create_child_tables(tablenames, stable, tags_lst):
    cut = 0
    conn = get_conn_tdengine()
    while cut < len(tablenames):
        _sql = "CREATE TABLE "
        _batch_sqls = []
        for tbname, tags in zip(
            tablenames[cut : cut + 3000], tags_lst[cut : cut + 3000]
        ):
            tags = [f"'{t}'" if isinstance(t, str) else str(t) for t in tags]
            tags_str = ", ".join(tags)
            _batch_sqls.append(
                f"IF NOT EXISTS {tbname} USING {stable} TAGS ({tags_str})"
            )
        _sql = _sql + " ".join(_batch_sqls) + ";"
        cut += 3000
        # print(_sql)
        conn.execute(_sql)


def _to_tdengine(
    conn, df: pd.DataFrame, tablename, stable=None, whole_df=True, types=None
):
    if df is None:
        return
    length = len(df)
    _tablename = get_tbname(tablename, stable=stable)
    if types is None:
        if stable is not None:
            types = table_types[stable]
        else:
            types = table_types[_tablename]
    col_names = df.columns
    values_len = len(col_names)
    _values = ", ".join(["?"] * values_len)
    _cols = ""
    if not whole_df:
        _cols = f'({",".join(col_names)})'
    _sql = f"INSERT INTO {_tablename} {_cols} VALUES({_values})"
    # print(_sql)
    stmt = conn.statement(_sql)
    if length == 1:
        values = taos.new_bind_params(values_len)
        i = 0
        for col in col_names:
            _type = types[col]
            if _type == "timestamp":
                data_ = int(df[col].iloc[0].timestamp() * 1000)
            else:
                data_ = df[col].iloc[0]
            param_funcs[_type](values[i], data_)
            i += 1
        stmt.bind_param(values)
    else:
        values = taos.new_multi_binds(values_len)
        i = 0
        for col in df.columns:
            _type = types[col]
            if _type == "timestamp":
                lst_ = [int(dt.timestamp() * 1000) for dt in df[col].to_list()]
            else:
                lst_ = df[col].to_list()
            # print(col, _type, df[col].dtype)
            param_funcs[_type](values[i], lst_)
            i += 1
        stmt.bind_param_batch(values)
    stmt.execute()
    stmt.close()


def insert(df: pd.DataFrame, tablename, stable=None, whole_df=True, types=None):
    conn = get_conn_tdengine()
    if len(df) > 30000:
        cut = 0
        while cut < len(df):
            _df = df.iloc[cut : cut + 30000]
            _to_tdengine(conn, _df, tablename, stable, whole_df, types)
            cut += 30000
    else:
        _to_tdengine(conn, df, tablename, stable, whole_df, types)
    logging.info(f"写入TDengine[{tablename}][{stable}]")


def insert_multi_tables(df, stable, whole_df=True, reorder_cols=True):
    if df.empty:
        return
    if "tablename" not in df:
        return
    types = table_types[stable]
    if reorder_cols:
        df = df[["tablename"] + list(types.keys())]
    df["tablename"] = df["tablename"].map(partial(get_tbname, stable=stable))
    col_names = list(df.columns)
    col_names.remove("tablename")
    values_len = len(col_names)
    _values = ", ".join(["?"] * values_len)
    _cols = ""
    if not whole_df:
        _cols = f'({",".join(col_names)})'
    conn = get_conn_tdengine()
    stmt = conn.statement(f"INSERT INTO ? {_cols} VALUES({_values})")
    tb_name = None
    for row in df.itertuples():
        if tb_name != row.tablename:
            tb_name = row.tablename.lower()
            # print(tb_name)
            stmt.set_tbname(tb_name)
        values: taos.TaosBind = taos.new_bind_params(values_len)
        i = 0
        for fld in row._fields:
            if fld == "tablename" or fld == "Index":
                continue
            data_ = getattr(row, fld)
            _type = types[fld]
            if _type == "timestamp":
                data_ = int(data_.timestamp() * 1000)
            param_funcs[_type](values[i], data_)
            i += 1
        stmt.bind_param(values)
    stmt.execute()
    stmt.close()
    logging.info(f"批量写入TDengine[{stable}]")


def insert_one(values, tablename):
    conn = get_conn_tdengine()
    values_len = len(values)
    _values = ", ".join(["?"] * values_len)
    types = table_types[tablename]
    stmt = conn.statement(f"INSERT INTO ? VALUES({_values})")
    stmt.set_tbname(tablename)
    params: taos.TaosBind = taos.new_bind_params(values_len)
    params[0].timestamp(int(values[0].timestamp() * 1000))
    flds = list(types.keys())
    for i in range(1, values_len):
        # print(flds[i])
        param_funcs[types[flds[i]]](params[i], values[i])
    stmt.bind_param(params)
    stmt.execute()
    stmt.close()
    logging.info(f"单表写入TDengine[{tablename}]")


def get_existed_tables(stable):
    conn = get_conn_tdengine()
    result = conn.query(f"SHOW TABLE TAGS FROM {stable};")
    rows = result.fetch_all_into_dict()
    existed_tables = set(row["tbname"] for row in rows)
    return existed_tables


def not_exist_tables(tables, stable):
    conn = get_conn_tdengine()
    existed_tables = get_existed_tables(conn, stable)
    tablenames = set([get_tbname(tb, stable=stable) for tb in tables])
    return list(tablenames - existed_tables)


def not_exist_symbols(symbols, stable):
    conn = get_conn_tdengine()
    result = conn.query(f"SHOW TABLE TAGS FROM {stable};")
    rows = result.fetch_all_into_dict()
    tablenames = set(symbols)
    existed_tables = set(row["symbol"] for row in rows)
    return list(tablenames - existed_tables)


def drop_tables(tables, stable):
    conn = get_conn_tdengine()
    _sql = ", ".join([f"IF EXISTS {get_tbname(tb, stable=stable)}" for tb in tables])
    conn.execute("DROP TABLE " + _sql)


def drop_row(tablename, dt, stable=None):
    conn = get_conn_tdengine()
    _sql = f"delete from {get_tbname(tablename, stable=stable)} where dt='{dt.isoformat()}'"
    conn.execute(_sql)


def drop_col(tablename, col):
    conn = get_conn_tdengine()
    _sql = f"ALTER TABLE {tablename} DROP COLUMN {col}"
    conn.execute(_sql)


if __name__ == "__main__":
    logging.basicConfig(level="DEBUG")

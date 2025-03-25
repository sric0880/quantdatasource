CREATE TABLE IF NOT EXISTS {tablename} AS
    SELECT * FROM read_csv('{csv_path}', header=true, columns = {{
        "dt":"TIMESTAMP",
        "open":"FLOAT",
        "high":"FLOAT",
        "low":"FLOAT",
        "close":"FLOAT",
        "avg_price":"FLOAT",
        "change":"FLOAT",
        "pct_change":"FLOAT",
        "volume":"UBIGINT",
        "turnover_rate":"FLOAT",
    }});

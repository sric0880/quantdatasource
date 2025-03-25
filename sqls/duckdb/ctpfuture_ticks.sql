CREATE TABLE IF NOT EXISTS {tablename} AS
    SELECT * FROM read_csv('{csv_path}', header=true, columns = {{
        "dt":"TIMESTAMP",
        "last_price":"FLOAT",
        "volume":"UINTEGER",
        "amount":"UINTEGER",
        "open_interest":"UINTEGER",
        "bid_price1":"FLOAT",
        "ask_price1":"FLOAT",
        "bid_volume1":"UINTEGER",
        "ask_volume1":"UINTEGER",
    }});
import pathlib

import yaml

raw_astock_output = ""
raw_future_output = ""
astock_output = ""
future_output = ""
tushare_token = ""
tq_username = ""
tq_psw = ""

ctp_accounts = []

with open("quantdatasource_config.yml", "r") as f:
    config = yaml.safe_load(f)
    raw_astock_output = config["raw_astock_output"]
    raw_future_output = config["raw_future_output"]
    astock_output = config["astock_output"]
    future_output = config["future_output"]
    pathlib.Path(raw_astock_output).mkdir(parents=True, exist_ok=True)
    pathlib.Path(raw_future_output).mkdir(parents=True, exist_ok=True)
    pathlib.Path(astock_output).mkdir(parents=True, exist_ok=True)
    pathlib.Path(future_output).mkdir(parents=True, exist_ok=True)
    tushare_token = config["tushare_token"]
    tq_username = config["tq_username"]
    tq_psw = config["tq_psw"]
    ctp_accounts = config["ctp_accounts"]
    print(config)

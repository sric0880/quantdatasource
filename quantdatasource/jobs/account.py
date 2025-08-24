import pathlib

astock_output = "datasource/AStock"
future_output = "datasource/CTPFuture"

pathlib.Path(astock_output).mkdir(parents=True, exist_ok=True)
pathlib.Path(future_output).mkdir(parents=True, exist_ok=True)

tushare_token = ""
tq_username = ""
tq_psw = ""

ctp_accounts = []

if config is None:
    with open("quantdatasource_config.yml", "r") as f:
        config = yaml.safe_load(f)
        astock_output = config["astock_output"]
        future_output = config["future_output"]
        tushare_token = config["tushare_token"]
        tq_username = config["tq_username"]
        tq_psw = config["tq_psw"]
        ctp_accounts = config["ctp_accounts"]
        print(config)

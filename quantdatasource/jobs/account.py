import pathlib

astock_output = "datasource/AStock"
future_output = "datasource/CTPFuture"

pathlib.Path(astock_output).mkdir(parents=True, exist_ok=True)
pathlib.Path(future_output).mkdir(parents=True, exist_ok=True)

tushare_token = ""
tq_username = ""
tq_psw = ""

ctp_accounts = [
    {
        "uname": "",
        "uid": "",
        "UserID": "",
        "BrokerID": "",
        "AppID": "",
        "AuthCode": "",
        "Password": "",
        "Name": "",
        "FrontAddresses": [],
    },
]

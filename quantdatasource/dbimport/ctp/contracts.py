import json
import logging


def read_contracts(json_file):
    logging.info(f"读取期货手续费及保证金")
    with open(json_file, "r") as f:
        contracts = json.load(f)
    for c in contracts:
        c["_id"] = c["instrument_id"]
        c.pop("instrument_id")
    if not contracts:
        logging.error(f"期货手续费及保证金为空")
        return []
    return contracts

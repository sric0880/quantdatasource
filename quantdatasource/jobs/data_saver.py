import logging

import pandas as pd
from quantdata import get_conn_mongodb


def _insert_many(coll, df):
    coll.insert_many(df.to_dict(orient="records"))


def _insert_many_ignore_nan(coll, df):
    data = df.to_dict(orient="records")
    for dct in data:
        del_ks = []
        for k, v in dct.items():
            if pd.isna(v):
                del_ks.append(k)
        for k in del_ks:
            dct.pop(k)
    coll.insert_many(data)


def mongo_delete_fields(coll, fields):
    new_ = {"$unset": {field: "" for field in fields}}
    coll.update_many({}, new_, False)


def mongo_insert_many(df, dbname, collection_name, ignore_nan=False, drop=True):
    logging.info(f"写入MongoDB[{dbname}][{collection_name}]")
    conn = get_conn_mongodb()
    db = conn[dbname]
    if drop:
        db.drop_collection(collection_name)
    if ignore_nan:
        _insert_many_ignore_nan(db[collection_name], df)
    else:
        _insert_many(db[collection_name], df)

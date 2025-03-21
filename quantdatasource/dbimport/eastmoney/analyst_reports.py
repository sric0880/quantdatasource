"""
东方财富数据源
导入：
    财报数据
依赖：
    交易日历
"""

import logging
import os

import numpy as np
import pandas as pd


def _read_analyst_reports(df):
    logging.info(f"读取研报")
    df["publishDate"] = pd.to_datetime(df["publishDate"])
    df = (
        df.drop(columns=["authorID", "author", "infoCode", "column", "encodeUrl"])
        .drop_duplicates()
        .replace(to_replace=r"^\s*$", value=np.nan, regex=True)
        .sort_values(by="publishDate")
        .reset_index(drop=True)
    )

    df["indvInduCode"] = df["indvInduCode"].fillna(0)
    df["emRatingCode"] = df["emRatingCode"].fillna(0)
    df["emRatingValue"] = df["emRatingValue"].fillna(0)
    df["lastEmRatingCode"] = df["lastEmRatingCode"].fillna(0)
    df["lastEmRatingValue"] = df["lastEmRatingValue"].fillna(0)
    df["ratingChange"] = df["ratingChange"].fillna(0)
    df["sRatingCode"] = df["sRatingCode"].fillna(0)
    df["count"] = df["count"].fillna(0)
    df = df.astype(
        {
            "title": "string",
            "stockName": "string",
            "stockCode": "string",
            "orgCode": "string",
            "orgName": "string",
            "orgSName": "string",
            "predictThisYearPe": "float64",
            "predictThisYearEps": "float64",
            "predictNextYearPe": "float64",
            "predictNextYearEps": "float64",
            "predictLastYearPe": "float64",
            "predictLastYearEps": "float64",
            "predictNextTwoYearPe": "float64",
            "predictNextTwoYearEps": "float64",
            "indvInduCode": "int32",
            "indvInduName": "string",
            "emRatingCode": "int16",
            "emRatingValue": "int16",
            "emRatingName": "string",
            "lastEmRatingCode": "int16",
            "lastEmRatingValue": "int16",
            "lastEmRatingName": "string",
            "ratingChange": "int8",
            "researcher": "string",
            "sRatingName": "string",
            "sRatingCode": "int32",
            "count": "int32",
        }
    )
    df["stockCode"] = df["stockCode"] + df["market"].map(
        {"SHENZHEN": ".SZ", "SHANGHAI": ".SH", "BEIJING": ".BJ", "OTHER": "NQ"}
    )  # NQ 新三板
    df = df.drop(
        columns=[
            "actualLastTwoYearEps",
            "actualLastYearEps",
            "reportType",
            "indvIsNew",
            "newListingDate",
            "newPurchaseDate",
            "attachType",
            "attachSize",
            "attachPages",
            "market",
            "orgType",
            "industryCode",
            "industryName",
            "emIndustryCode",
        ]
    )
    # print(df)
    # print(df.info())
    return df


def read_analyst_reports(output, date_start, date_end, cal):
    df = pd.read_csv(
        os.path.join(output, f"analyst_reports_{date_start}_{date_end}.csv"),
        encoding="utf-8",
        dtype={"stockCode": "string"},
        index_col=0,
    )
    df = _read_analyst_reports(df)
    # 将非交易日发布的研报日期改到上一个交易日日期
    df["tradingDate"] = df["publishDate"].map(
        lambda x: pd.Timestamp(cal.get_tradeday_last(x.to_pydatetime()), unit="s")
    )
    return df


def addition_read_analyst_reports(analyst_reports_addition_path, cal):
    df = pd.read_csv(
        analyst_reports_addition_path,
        encoding="utf-8",
        dtype={"stockCode": "string"},
        index_col=0,
    )
    df = _read_analyst_reports(df)
    df["tradingDate"] = pd.Timestamp(
        cal.get_tradeday_last(df["publishDate"].iloc[-1].to_pydatetime()), unit="s"
    )
    return df

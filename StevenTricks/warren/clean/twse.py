#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 20 21:13:14 2020

@author: stevenhsu
"""
import os.path
import random
from copy import deepcopy
import pandas as pd
import numpy as np
import requests as re
from os.path import join
from StevenTricks.snt import findbylist
from StevenTricks.fileop import PathWalk_df, pickleload, filename
from StevenTricks.warren.twse import Log
from StevenTricks.warren.conf import db_path, colname_dic, numericol, collection, dropcol
import datetime


def addcolumns(df):
    # 開始添加、變更欄位=======================================
    if "買進金額" in df and "賣出金額" in df:
        df.loc[:, "交易總額"] = df["買進金額"] + df["賣出金額"]
    if "外陸資買進股數_不含外資自營商" in df and "外陸資賣出股數_不含外資自營商" in df:
        df.loc[:, "外陸資交易總股數_不含外資自營商)"] = df["外陸資買進股數_不含外資自營商"] + df[
            "外陸資賣出股數_不含外資自營商"]
    if "外資買進股數" in df and "外資賣出股數" in df:
        df.loc[:, "外資交易總股數"] = df["外資買進股數"] + df["外資賣出股數"]
    if "外陸資買賣超股數_不含外資自營商" in df and "外資自營商買賣超股數" in df:
        df.loc[:, "外陸資買賣超股數"] = df["外陸資買賣超股數_不含外資自營商"] + df["外資自營商買賣超股數"]
    if "投信買進股數" in df and "投信賣出股數" in df:
        df.loc[:, "投信交易總股數"] = df["投信買進股數"] + df["投信賣出股數"]
    if "自營商買進股數_自行買賣" in df and "自營商賣出股數_自行買賣" in df:
        df.loc[:, "自營商交易總股數_自行買賣"] = df["自營商買進股數_自行買賣"] + df["自營商賣出股數_自行買賣"]
    if "自營商買進股數_避險" in df and "自營商賣出股數_避險" in df:
        df.loc[:, "自營商交易總股數_避險"] = df["自營商買進股數_避險"] + df["自營商賣出股數_避險"]
    if "收盤價" in df and "本益比" in df:
        df.loc[:, "eps"] = df["收盤價"] / df["本益比"]
    if "成交金額" in df and "成交筆數" in df:
        df.loc[:, "成交金額/成交筆數"] = df["成交金額"] / df["成交筆數"]
    if "成交股數" in df and "成交筆數" in df:
        df.loc[:, "成交股數/成交筆數"] = df["成交股數"] / df["成交筆數"]
    if "融資買進" in df and "融資賣出" in df and "現金償還" in df:
        df.loc[:, "融資交易總張數"] = df["融資買進"] + df["融資賣出"] + df["現金償還"]
    if "融券買進" in df and "融券賣出" in df and "現券償還" in df:
        df.loc[:, "融券交易總張數"] = df["融券買進"] + df["融券賣出"] + df["現券償還"]
    if "當日賣出" in df and "當日還券" in df and "當日調整" in df:
        df.loc[:, "借券交易總股數"] = df["當日賣出"] + df["當日還券"] + df["當日調整"]
    if "前日融資餘額" in df and "今日融資餘額" in df:
        df.loc[:, "淨融資"] = df["今日融資餘額"] - df["前日融資餘額"]
    if "前日融券餘額" in df and "今日融券餘額" in df:
        df.loc[:, "淨融券"] = df["今日融券餘額"] - df["前日融券餘額"]
    if "前日餘額" in df and "當日餘額" in df:
        df.loc[:, "淨借券"] = df["當日餘額"] - df["前日餘額"]
    if "融券交易張數" in df and "融券交易張數" in df:
        df.loc[:, "信用交易總張數"] = df["融券交易張數"] + df["融資交易張數"]
    if "信用交易總張數" in df:
        df.loc[:, "信用交易淨額"] = df["融資交易張數"] - df["融券交易張數"]
    if "買進" in df and "賣出" in df and "現金(券)償還" in df:
        df.loc[:, "信用交易總額"] = df["買進"] + df["賣出"] + df["現金券償還"]
    if "整體市場" in df and "股票" in df:
        df.loc[:, ["整體市場", "整體市場漲停"]] = df["整體市場"].str.split("(", expand=True).rename(
            columns={0: "整體市場", 1: "整體市場漲停"})
        df.loc[:, ["股票", "股票漲停"]] = df["股票"].str.split("(", expand=True).rename(
            columns={0: "股票", 1: "股票漲停"})
        df.replace("\)", "", regex=True, inplace=True)
        df = turntofloat(df, col=["整體市場", "整體市場漲停", "股票", "股票漲停"])
    return df


def stocktable_combine(df=pd.DataFrame([]), stocktable=pd.DataFrame([])):
    if "代號" in df and "名稱" in df:
        df.index = df["代號"].str.strip() + "_" + df["名稱"].str.strip()
    df = df.join(stocktable.loc[:, [_ for _ in stocktable if _ not in df]])
    df.dropna(axis=1, how="all", inplace=True)
    return df


productkey = {
    "col": ["field"],
    "value": ["data", "list"],
    "title": ["title"]
        }


def getkeys(data):
    productcol = {
        "col": [],
        "value": [],
        "title": [],
        }
    for key in sorted(data.keys()):
        for k, i in productkey.items():
            i = [key for _ in i if _ in key.lower()]
            if i:
                productcol[k] += i
    return pd.DataFrame(productcol)


def productdict(source, key):
    productdict = {}
    for col, value, title in key.values:
        if not source[value]:
            continue
        df = pd.DataFrame(data=source[value], columns=source[col])
        productdict[source[title]] = df
    return productdict


def type1(df, title, subtitle):
    df = df.replace({",": "", r'\)': ''}, regex=True)
    df = df.rename(columns=colname_dic)
    df = df.drop(columns=dropcol, errors='ignore')
    df.loc[:, numericol[title][subtitle]] = df[numericol[title][subtitle]].apply(pd.to_numeric, errors='coerce')
    return {subtitle: df}


def type2(df, title, subtitle):
    # 處理xxx(yy)的格式，可以把xxx和(yy)分開成上下兩列
    res = []
    for subcol in df:
        res.append(df[subcol].str.split(r'\(', expand=True, regex=True).rename(columns={0: subcol}))
    res = pd.concat(res, axis=1)
    df = res.drop(1, axis=1)
    res = res.loc[:, 1]
    res.columns = df.columns
    df = pd.concat([df, res], ignore_index=True).dropna()
    df = type1(df, title=title, subtitle=subtitle)
    return df


fundic = {
    '每日收盤行情': {
        '價格指數(臺灣證券交易所)': type1,
        '價格指數(跨市場)': '',
        '價格指數(臺灣指數公司)': '',
        '報酬指數(臺灣證券交易所)': type1,
        '報酬指數(跨市場)': '',
        '報酬指數(臺灣指數公司)': '',
        '大盤統計資訊': type1,
        '漲跌證券數合計': type2,
        '每日收盤行情': ''
    }
}


def cleaner(data, title):
    # data 就是直接讀取pkl檔案得到的data
    # title就是大標，pkl檔案裡面有subtitle小標
    # pkl的小標資料不乾淨，需要透過轉換，所以就有find
    # 返回的資料會是dict{subtitle:df}
    res = {}
    keydf = getkeys(data)
    product = productdict(source=data, key=keydf)
    for key, df in product.items():
        find = findbylist(data['crawlerdic']['subtitle'], key)
        # 把小標做轉換成find
        if find:
            if len(find) > 1:
                print('{} is in {} at the same time.'.format(key, ','.join(find)))
                break
            else:
                fun = fundic[title][find[0]]
                res[find[0]] = fun(data, title, find[0])
        else:
            print('{} is not in crawlerdic.SubItem.'.format(key))
            break
    return res


if __name__ == '__main__':
    stocklog = Log(db_path)
    log = stocklog.findlog('source', 'log.pkl')
    files = PathWalk_df(path=join(db_path, 'source'), fileexclude=['log'], fileinclude=['.pkl'])

    for path in files['path']:
        title = filename(path)
        res = cleaner(data=pickleload(path=path), title=title)


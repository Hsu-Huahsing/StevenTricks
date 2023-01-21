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
from StevenTricks.warren.conf import db_path, colname_dic, numericol
import datetime
mode = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
res = re.get(stocklist['url'].format(str(1)))
r = pd.read_html(stocklist['url'].format(str(2)), encoding='cp950')
r = pd.DataFrame(r[0])
a=r.reset_index(drop=True).reset_index()
r.loc[r.duplicated(keep=False), 'product']
r.T
pd.DataFrame(res)






def get_item(title):
    return title_dic[title]


def get_title(item):
    return crawlerdic[item]["title"]


def search_title(item, title):
    res = [_ for _ in crawlerdic[item]["title"] if _ in title]
    return res


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


def type1(df, key):
    df = df.replace({",":""},regex=True)
    df = df.rename(columns=colname_dic)
    df = df[numericol[key]].apply(pd.to_numeric, errors='coerce')
    return df


def cleaner(data):
    keydf = getkeys(data)
    productdict()


fundic = {

}


if __name__ == '__main__':
    stocklog = Log(db_path)
    log = stocklog.findlog('source', 'log.pkl')
    files = PathWalk_df(path=join(db_path, 'source'), fileexclude=['log'], fileinclude=['.pkl'])
    filedict = pickleload(path=files['path'][0])
    splitext(files['path'][0])
    filedict
    filedict.keys()
    pd.to_datetime(filedict['date'])
    filedict['crawlerdic']['SubItem']
    keydf = getkeys(filedict)
    productdict = productdict(source=filedict, key=keydf)
    import re
    for key, df in productdict.items():
        find = findbylist(filedict['crawlerdic']['SubItem'], key)
        if find:
            df = type1(df, colname_dic.get(find[0], find[0]))
        else:
            break
        print(key, df)



    filedict.keys()
    filedict['date']

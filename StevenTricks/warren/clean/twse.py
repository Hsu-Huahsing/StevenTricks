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
from os.path import join, exists
from StevenTricks.snt import findbylist
from StevenTricks.fileop import PathWalk_df, pickleload, filename, logfromfolder, picklesave
from StevenTricks.dfi import findval, dfrows_iter
from StevenTricks.warren.twse import Log
from StevenTricks.warren.conf import db_path, colname_dic, numericol, collection, dropcol, datecol
from StevenTricks.dbsqlite import tosql_df, readsql_iter
from StevenTricks.snt import tonumeric_int
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
    df = df.replace({",": ""}, regex=True)
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
    df = df.replace({r'\)': ''}, regex=True)
    df = type1(df, title=title, subtitle=subtitle)
    return df


fundic = {
    '每日收盤行情': {
        '價格指數(臺灣證券交易所)': type1,
        '價格指數(跨市場)': type1,
        '價格指數(臺灣指數公司)': type1,
        '報酬指數(臺灣證券交易所)': type1,
        '報酬指數(跨市場)': type1,
        '報酬指數(臺灣指數公司)': type1,
        '大盤統計資訊': type1,
        '漲跌證券數合計': type2,
        '每日收盤行情': type1,
    }
}





def cleaner(product, title):
    # data 就是直接讀取pkl檔案得到的data
    # title就是大標，pkl檔案裡面有subtitle小標
    # pkl的小標資料不乾淨，需要透過轉換，所以就有find
    # 返回的資料會是dict{subtitle:df}
    res = {}
    for key, df in product.items():
        find = findbylist(collection[title]['subtitle'], key)
        # 把小標做轉換成find
        if find:
            if len(find) > 1:
                print('{} is in {} at the same time.'.format(key, ','.join(find)))
                break
            else:
                print(find[0], title, df)
                fun = fundic[title][find[0]]
                res.update(fun(df, title, find[0]))
        else:
            print('{} is not in crawlerdic.SubItem.'.format(key))
            break
    return res


if __name__ == '__main__':
    # a=pickleload(r'/Users/stevenhsu/Library/Mobile Documents/com~apple~CloudDocs/warehouse/stock/source/stocklist/2/股票/股票_2023-01-27.pkl')
    stocklog = Log(db_path)
    # 初始化
    log = stocklog.findlog('source', 'log.pkl')
    # 讀取log
    log_stocklist_path = join(db_path, 'source', 'stocklistlog.pkl')
    # 設定log_stocklist的路徑
    if exists(log_stocklist_path) is True:
        log_stocklist = pickleload(path=log_stocklist_path)
        log_stocklist = logfromfolder(join(db_path, 'source', 'stocklist'), fileinclude=['.pkl'], fileexclude=['log'],
                                      dirinclude=['stocklist'], direxclude=[], log=log_stocklist, fillval='succeed', avoid=['cleaned'])
    else:
        log_stocklist = logfromfolder(join(db_path, 'source', 'stocklist'), fileinclude=['.pkl'], fileexclude=['log'], dirinclude=['stocklist'], direxclude=[], log=pd.DataFrame(), fillval='succeed')
    # 讀取stocklist的log
    files = PathWalk_df(path=join(db_path, 'source'), direxclude=['stocklist'], fileexclude=['log'], fileinclude=['.pkl'])
    # 一般檔案的path
    files_stocklist = PathWalk_df(path=join(db_path, 'source'), dirinclude=['stocklist'], fileexclude=['log'], fileinclude=['.pkl'])
    # stocklist的檔案path

    # n=0
    for ind, col in findval(log_stocklist, 'succeed'):
        # print(ind, col)
        # if n==3:break
        # n+=1
        # 用succeed當條件
        data = pickleload(files_stocklist.loc[files_stocklist['file'] == '{}_{}.pkl'.format(col, ind), 'path'].values[0])
        # 讀取檔案當作data
        if data.empty is True:
            # 有些下載下來本身就是空值，要做特殊處理，直接跳過，但是要做log紀錄
            log_stocklist.loc[ind, col] = 'cleaned'
            picklesave(data=log_stocklist, path=log_stocklist_path)
            continue
        data = data.rename(columns=colname_dic)
        # 開始欄位rename
        for key in ['指數代號及名稱', '有價證券代號及名稱']:
            # 名稱欄位要把代號和名稱拆開成兩欄
            if key in data :
                data.loc[:,['代號','名稱']] = data[key].str.split(r'\u3000', expand=True).rename(columns={0:'代號',1:'名稱'})
                # \u3000是全形的空白鍵，就算前面不加r也能判斷成功，但怕以後會不能用｜去做多重判斷，所以先放r
                data = data.drop(key, axis=1)
                # 最後要drop本來的key
        # 以上之後可以考慮做成function
        if tonumeric_int(col[-1]) is not None:
            col = col[:-1]
        #     把尾數的數字篩選掉
        colrename = colname_dic.get(col, col)
        # 欄位的rename
        data.loc[:, ['type', 'date']] = colrename, ind
        # 新增兩個欄位
        data.loc[:, [_ for _ in numericol['stocklist'] if _ in data]] = data[[_ for _ in numericol['stocklist'] if _ in data]].apply(pd.to_numeric, errors='coerce')
        # 利率值是空的就代表是浮動利率
        data.loc[:, [_ for _ in datecol['stocklist'] if _ in data]] = data[[_ for _ in datecol['stocklist'] if _ in data]].apply(pd.to_datetime, errors='coerce')
        # 到期日，日期是空的就代表無到期日
        tosql_df(df=data, dbpath=join(db_path, 'cleaned', 'stocklist.db'), table=colrename, pk=["ISINCode"])
        # 放進db，用最簡單的模式，直覺型放入，沒有用adapter
        log_stocklist.loc[ind,col] = 'cleaned'
        # 成功放進db之後就要改成cleaned
        picklesave(data=log_stocklist, path=log_stocklist_path)
        # 儲存log

    stocklist = pd.concat(readsql_iter(dbpath=join(db_path, 'cleaned', 'stocklist.db')))
    # 讀取stocklist，以利下面可以merge


    n = 1
    for path in files['path']:
        if n == 2:
            break
        n += 1
        title, date = filename(path).split('_')[0], filename(path).split('_')[1]
        # 拿到檔名分隔號＿的前半部當作title、後半部當作date
        file = pickleload(path=path)
        # 讀取pkl檔案
        keydf = getkeys(file)
        # 找到所有key對應的資料
        product = productdict(source=file, key=keydf)
        # 把key對應的結果和product合併起來
        res = cleaner(product=product, title=title)
        # 清理結果要取出
        for key, df in res.items():
            # merge就是優先用代號，沒有代號就用名稱
            if key not in collection[title]['combinepk']:
                if '代號' in df:
                    df = df.merge(stocklist.loc[:, [_ for _ in stocklist if _ not in df.drop('代號', axis=1)]], how='left', on=['代號'])
                    pk = ['代號']
                else:
                    df = df.merge(stocklist.loc[:, [_ for _ in stocklist if _ not in df.drop('名稱', axis=1)]], how='left', on=['名稱'])
                    pk = ['名稱']
            else:
                pk = ['名稱', 'date']

            key = colname_dic.get(key, key)
            # key的轉換主要是把括號弄掉和一些常用字的轉換
            df.loc[:, ['date', 'table']] = date, key
            # 全部都要新增日期，就算有merge，這裡也要把stocklist裏面的date覆蓋掉，table就是等一下放盡sqldb要用的table name

            tosql_df(df=df, dbpath=join(db_path, 'cleaned', '{}.db'.format(date.split('-')[0])), table=key, pk=pk)





import pandas as pd
from StevenTricks.dictur import findstr
from StevenTricks.fileop import pickleload, picklesave, warehouseinit
from StevenTricks.dfi import periodictable
from StevenTricks.warren.conf import stocklist, colname_dic
from StevenTricks.warren.conf import collection
from StevenTricks.netGEN import randomheader, safereturn
import requests as re
# from sys import path
from os.path import exists, join
from datetime import datetime
datetime.now()


class Log:
    def __init__(self, warehousepath=''):
        self.warehousepath = warehousepath
        warehouseinit(self.warehousepath)

    def findlog(self, logtype, kind):
        # logtype could be 'source' 'cleaned' ''
        # kind could be 'log.pkl' 'errorlog.pkl'
        # print(join(self.warehousepath, logtype, kind))
        if exists(join(self.warehousepath, logtype, kind)) is True:
            print('exists')
            return pickleload(join(self.warehousepath, logtype, kind))
        return None

    def updatelog(self, periodictdf, periodict):
        if periodictdf is None:
            log = periodictable(periodict)
        else:
            latestlog = periodictable(periodict, datemin=datetime.now())
            log = pd.concat([periodictdf, latestlog])
        return log

    def savelog(self, log, logtype, kind):
        # logtype could be 'source'、'cleaned'，也可以什麼都不打 '' ，就代表是warehouse底下的使用紀錄
        # kind could be 'log.pkl' 'errorlog.pkl'
        path = join(self.warehousepath, logtype, kind)
        picklesave(log, path)

if __name__ == '__main__':
    pass


    def stocktablecrawl(maxn=13, timeout=180, pk="ISINCode"):
        # maxn 是指這個網頁支援的產品類型總類，目前最多到12，因此預設是13
        # dm = dbmanager(user="root")
        # dm.choosedb(db="stocktable")

        for _ in range(1, maxn, 1):
            df = pd.read_html(stocklist['url'].format(str(_)), encoding='cp950')
            df = pd.DataFrame(df[0])

            if df.empty is True:
                print("stocktable No:{} ___empty crawled result".format(str(_)))
                continue

            df = df.reset_index(drop=True).reset_index()
            # 先弄出一列是連續數字出來
            tablename = [list(set(_)) for _ in df.values if len(set(_)) == 2]
            # 要找出一整列都是重複的，當作table name，因為剛剛已經用reset_index用出一整數列了，得出的重複值會長這樣[3,重複值]，所以如果是我們要找的重複值，最少會有兩個值，一個是數列，一個是重複值
            df = df.drop(["index", "Unnamed: 6"], errors="ignore", axis=1)
            # 把用不到的數列先刪掉，包括剛剛的index
            df.loc[:, "date"] = datetime.now().date()
            # 增加一列日期

            # 以下對特殊欄位進行特殊處理
            if "指數代號及名稱" in df:
                df.loc[:, ["代號", "名稱"]] = df.loc[:, "指數代號及名稱"].str.split(" |　", expand=True, n=1).rename(
                    columns={0: "代號", 1: "名稱"})
            elif "有價證券代號及名稱" in df:
                df.loc[:, ["代號", "名稱"]] = df.loc[:, "有價證券代號及名稱"].str.split(" |　", expand=True, n=1).rename(
                    columns={0: "代號", 1: "名稱"})

            df = df.rename(columns=colname_dic)
            # 把處理好的欄位重新命名

            if pk not in df:
                print("no primary key")
                print(_)
                print(pk)
                print(df.columns)
                continue
            # 以上檢查是否有變更primary key欄位的狀況

            if len(tablename) > 1:
                name_index = [(a, b) for a, b in zip(tablename, tablename[1:] + [[None]])]
            elif len(tablename) == 1:
                name_index = [(tablename[0], [None])]
            else:
                table = "無細項分類的商品{}".format(str(_))
                df.loc[:, "product"] = table
                dm.to_sql_ex(df=df, table=table, pk=pk)
                continue
            # 利用同一個row的重複值來判斷商品項目名稱，同時判斷儲存的方式

            for nameindex in name_index:
                start = nameindex[0]
                end = nameindex[1]

                startname, startint = [_ for _ in start if isinstance(_, str) is True][0], \
                    [_ for _ in start if isinstance(_, str) is False][0]
                endint = [_ for _ in end if isinstance(_, str) is False][0]
                # 先抓出起始的值和尾端值然後用slice來做切割，把資料分段儲存進table

                if end[0] is None:
                    df_sub = df[startint + 1:]
                else:
                    df_sub = df[startint + 1:endint]

                if startname in rename_dic: startname = rename_dic[startname]
                df_sub.loc[:, "product"] = startname
                dm.to_sql_ex(df=df_sub, table=startname, pk=pk)


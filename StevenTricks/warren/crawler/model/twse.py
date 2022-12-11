import pandas as pd
from StevenTricks.dictur import findstr
from StevenTricks.fileop import pickleload, picklesave, warehouseinit
from StevenTricks.dfi import periodictable
from StevenTricks.warren.conf import collection
from StevenTricks.netGEN import randomheader, safereturn
import requests as re
# from sys import path
from os.path import exists, join
from datetime import datetime
datetime.now()


class Log:
    def __init__(self, warehousepath=''):
        self.warehouse = warehousepath
        warehouseinit(self.warehouse)

        # 先把三個log初始化成None
        self.sourcelog = None
        self.cleanedlog = None
        self.usagelog = None

        # 再去檢查是否有檔案，有的話就讀取，沒有就保持None
        if exists(join(warehousepath, 'source', 'log.pkl')) is True:
            self.sourcelog = pickleload(join(warehousepath, 'source', 'log.pkl'))
        if exists(join(warehousepath, 'cleaned', 'log.pkl')) is True:
            self.sourcelog = pickleload(join(warehousepath, 'cleaned', 'log.pkl'))
        if exists(join(warehousepath, 'log.pkl')) is True:
            self.sourcelog = pickleload(join(warehousepath, 'log.pkl'))

    def updatelog(self, periodictdf, periodict):
        if periodictdf is None:
            log = periodictable(periodict)
        else:
            latestlog = periodictable(periodict, datemin=datetime.now())
            log = pd.concat([periodict, latestlog])
        return log

    def savelog(self, log, logtype):
        # logtype could be 'source'、'cleaned'，也可以什麼都不打，就代表是warehouse底下的使用紀錄
        path = join(self.warehouse, logtype, r'log')
        picklesave(log, path)


if __name__ == '__main__':
    pass
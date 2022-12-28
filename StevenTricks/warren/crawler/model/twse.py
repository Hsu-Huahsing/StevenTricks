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
        self.warehousepath = warehousepath
        warehouseinit(self.warehousepath)

    def findlog(self, logtype, kind):
        # logtype could be 'source' 'cleaned' ''
        # kind could be 'log.pkl' 'errorlog.pkl'
        print(join(self.warehousepath, logtype, kind))
        if exists(join(self.warehousepath, logtype, kind)) is True:
            return pickleload(join(self.warehousepath, logtype, kind))
        return None

    def updatelog(self, periodictdf, periodict):
        if periodictdf is None:
            log = periodictable(periodict)
        else:
            latestlog = periodictable(periodict, datemin=datetime.now())
            log = pd.concat([periodict, latestlog])
        return log

    def savelog(self, log, logtype, kind):
        # logtype could be 'source'、'cleaned'，也可以什麼都不打 '' ，就代表是warehouse底下的使用紀錄
        # kind could be 'log.pkl' 'errorlog.pkl'
        path = join(self.warehousepath, logtype, kind)
        picklesave(log, path)


if __name__ == '__main__':
    pass
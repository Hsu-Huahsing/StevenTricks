import pandas as pd
from StevenTricks.dictur import findstr
from StevenTricks.fileop import pickleload, picklesave, warehouseinit
from StevenTricks.dfi import periodictable
from StevenTricks.warren.conf import collection
from StevenTricks.netGEN import randomheader, safereturn
import requests as re
# from sys import path
from os.path import exists, join


class Log:
    def __init__(self,warehousepath=''):
        self.warehouse = warehousepath
        warehouseinit(self.warehouse)

        self.sourcelog = None
        self.cleanedlog = None
        self.usagelog = None
        if exists(join(warehousepath, 'source', 'log.pkl')) is True:
            self.sourcelog = pickleload(join(warehousepath, 'source', 'log.pkl'))
        if exists(join(warehousepath, 'cleaned', 'log.pkl')) is True:
            self.sourcelog = pickleload(join(warehousepath, 'cleaned', 'log.pkl'))
        if exists(join(warehousepath, 'log.pkl')) is True:
            self.sourcelog = pickleload(join(warehousepath, 'log.pkl'))

    def updatetable(self, logtype):
        #logtype可以是source、cleanedlog、usagelog





if __name__ == '__main__':
    pass
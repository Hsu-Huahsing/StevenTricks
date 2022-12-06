import pandas as pd

from StevenTricks.dictur import findstr
from StevenTricks.fileop import pickleload, picklesave, warehouseinit
from StevenTricks.dfi import dateseries
from StevenTricks.warren.conf import collection
from StevenTricks.netGEN import randomheader, safereturn
import requests as re
# from sys import path
from os.path import exists, join


class log:
    def __int__(self, warehousepath):
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

    def periodictable(self, perioddict):
        # 傳入的格式為{name:{'mindate':'yyyy-m-d','freq':'D' or 'M'}}，可多個name同時傳入
        df = []
        for key in perioddict:
            df.append(dateseries(seriesname=key, mindate=perioddict[key]['mindate'], freq=perioddict[key]['freq'], defaultval=False))
        df = pd.concat(df, axis=1)
        return df


if __name__ == '__main__':
    pass
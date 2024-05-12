from StevenTricks.fileop import pickleload, picklesave, warehouseinit
from StevenTricks.dfi import periodictable

from os.path import exists, isfile, basename, dirname, isdir, splitext, join, getmtime
from os import stat
from datetime import datetime, date, timedelta
import pandas as pd


class PathSweeper:
    def __init__(self, path):
        self.path = path
        self.file_dir = None
        self.file = None
        self.file_exists = False
        self.file_dirname = None
        self.file_name = None
        self.file_ext = None
        self.file_bldg = None
        self.file_mod = None
        self.file_atime = None
        self.file_ctime = None
        self.file_mtime = None

        if exists(path) is True:
            if isfile(path) is True:
                temp_stat = stat(path)
                self.file_exists = True
                self.file = basename(path)
                self.file_dir = dirname(path)
                self.file_dirname = basename(self.file_dir)
                self.file_name, self.file_ext = splitext(self.file)
                self.file_atime = datetime.fromtimestamp(temp_stat.st_atime)
                self.file_ctime = datetime.fromtimestamp(temp_stat.st_ctime)
                self.file_mtime = datetime.fromtimestamp(temp_stat.st_mtime)
            elif isdir(path) is True:
                self.file_dir = path

    def report(self):
        # report in series type, because it could be convenient to concat with other series
        return pd.Series(self.__dict__)


def logmaker(write_dt, data_dt, log=pd.Series(dtype='object'),  period=None, index=None):
    # log就是額外想要加入的資訊，格式固定是series
    # write_dt就是寫入當下的時間點
    # data_dt就是資料的時間
    # period就是資料的更新週期，目前支援日、月、年
    # index就是項目的名字
    if period == "day":
        period = data_dt
    elif period == "month":
        period = str(data_dt).rsplit("-", 1)[0]
    elif period == "year":
        period = str(data_dt.year)
    return pd.concat([pd.Series({"write_dt": write_dt, "data_dt": data_dt, "period": period, "index": index}, dtype='object'),
                      log], axis=1).dropna(how="any", axis=1)


class Log:
    # 要先執行findlog才能找到log的路徑，和相關資訊，如果只是呼叫class不會有額外的資訊
    def __init__(self, warehousepath=''):
        self.warehousepath = warehousepath
        warehouseinit(self.warehousepath)
        self.log_mtime = None
        self.log_path = r""
        self.logerror_mtime = None
        self.logerror_path = r""
    def findlog(self, logtype, kind, periodict=None):
        # logtype could be 'source' 'cleaned' ''
        # kind could be 'log.pkl' 'errorlog.pkl'
        # print(join(self.warehousepath, logtype, kind))
        # 找到或是重新創建一個新的log，periodict可以用來建立預設的時間點表格
        if exists(join(self.warehousepath, logtype, kind)) is True:
            print('The old log exists')
            log = pickleload(join(self.warehousepath, logtype, kind))

            if str(datetime.today().date()) not in log.index and periodict is not None:
                print("{} not in log index, updating the log table".format(str(datetime.today().date())))
                latestlog = periodictable(periodict, datemin=log.index.max()+timedelta(days=1))
                # 從上一次創建log的最新天數開始，所以要加一天，然後開始創建新的table
                log = pd.concat([log, latestlog])
        else:
            if kind == 'log.pkl':
                if periodict is not None:
                    log = periodictable(periodict)
                    print("Creating the new log table")
                    picklesave(log, join(self.warehousepath, logtype, kind))
                else:
                    print("There is not the old log, and it need the predict to create the new log table")
                    print("Stop the process")
                    exit(0)
            elif kind == 'error.pkl':
                log = pd.DataFrame()
                picklesave(log, join(self.warehousepath, logtype, kind))

        if kind == 'log.pkl':
            self.log_path = join(self.warehousepath, logtype, kind)
            self.log_mtime = datetime.fromtimestamp(getmtime(self.log_path))
        elif kind == 'error.pkl':
            self.logerror_path = join(self.warehousepath, logtype, kind)
            self.logerror_mtime = datetime.fromtimestamp(getmtime(self.logerror_path))

        return log

    def savelog(self, log, logtype, kind):
        # logtype could be 'source'、'cleaned'，也可以什麼都不打 '' ，就代表是warehouse底下的使用紀錄
        # kind could be 'log.pkl' 'errorlog.pkl'
        path = join(self.warehousepath, logtype, kind)
        picklesave(log, path)


if __name__ == '__main__':
    pass
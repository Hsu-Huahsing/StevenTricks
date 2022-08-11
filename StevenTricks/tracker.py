from os.path import exists, isfile, basename, dirname, isdir, splitext
from os import stat
from datetime import datetime
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


class tracer():
    def __init__(self, log_path):
        self.log_path = log_path
        self.log_stat = PathSweeper(log_path)
        self.log_exists = self.log_stat.file_exists


if __name__ == '__main__':
    pass
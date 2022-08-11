from os.path import exists, isfile, basename, dirname, isdir, splitext
from os import stat
from datetime import datetime
import pandas as pd

class tracer:
    def __init__(self, path):
        self.path = path
        self.file_dir = None
        self.file = None
        self.file_exist = False
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
                self.file_exist = True
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
        return pd.DataFrame(self.__dict__)

# splitext(r'/Users/stevenhsu/Library/Mobile Documents/com~apple~CloudDocs/dreamland/StevenTricks/dfi.py')[0]
a = tracer(r'/Users/stevenhsu/Library/Mobile Documents/com~apple~CloudDocs/dreamland/StevenTricks/dfi.py')
a.report()
a.file_name
a.file
a.file_ext
a.__dict__

if __name__ == '__main__':
    pass
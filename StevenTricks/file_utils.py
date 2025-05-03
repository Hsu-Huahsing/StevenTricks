# -*- coding: utf-8 -*-
"""
Created on Tue Apr 26 15:53:48 2022

@author: 118939
"""
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 26 15:53:48 2022
@author: 118939
"""

import pandas as pd
from os import makedirs, walk, remove, getcwd
from os.path import exists, pardir, abspath, isfile, samefile, join, splitext, dirname, basename, getmtime
from datetime import datetime
import pickle
from pathlib import Path

# 顯示目前執行的時間與執行檔案的名稱
def runninginfo():
    t = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    res = {"Time": t, "File": ""}
    try:
        res["File"] = __file__
    except NameError:
        pass
    print("在{}/n執行{}".format(t, res["File"]))
    return res

# 使用 pickle 將 Python 物件儲存到指定路徑
def picklesave(data, path):
    makedirs(abspath(dirname(path)), exist_ok=True)
    with open(path, 'wb') as f:
        pickle.dump(data, f)

# 從 pickle 檔案載入 Python 物件
def pickleload(path):
    with open(path, 'rb') as f:
        return pickle.load(f)

# 建立包含 'source' 和 'cleaned' 子目錄的資料夾結構
def warehouseinit(path):
    source, cleaned = join(path, 'source'), join(path, 'cleaned')
    makedirs(source, exist_ok=True)
    makedirs(cleaned, exist_ok=True)

# 將 HTML Excel 轉為 .xlsx，並刪除原檔案
def xlstoxlsx(path):
    newpath = splitext(path)[0] + '.xlsx'
    with pd.ExcelWriter(newpath) as writer:
        for df in pd.read_html(path, header=0):
            df.to_excel(writer, index=False)
    remove(path)
    return newpath

# 避免覆蓋檔案，自動遞增檔名（_duplicatedN）
def independentfilename(root, mark="_duplicated", count=1):
    if exists(root):
        ext = splitext(root)[1]
        root = splitext(root)[0]
        if mark in root:
            rootsplit = root.split(mark)
            root = rootsplit.pop(0)
            if rootsplit:
                count = int(rootsplit.pop(0)) + 1
        root += mark + str(count) + ext
        if exists(root):
            root = independentfilename(root)
    return root

# 計算兩個資料夾的相對階層距離
def pathlevel(left, right):
    if isfile(right):
        right = abspath(join(right, pardir))
    if len(left) > len(right):
        return
    level = 0
    while not samefile(left, right):
        right = abspath(join(right, pardir))
        level += 1
    return level

# 遍歷資料夾並回傳符合條件的檔案 DataFrame
def PathWalk_df(path, dirinclude=[], direxclude=[], fileexclude=[], fileinclude=[], level=None):
    res = []
    for _path, dire, file in walk(path):
        if not dire and not file:
            res.append([None, path])
        for f in file:
            res.append([f, join(_path, f)])
    res = pd.DataFrame(res, columns=["file", "path"])
    res["level"] = res["path"].map(lambda x: pathlevel(path, x))
    if level is not None:
        res = res.loc[res["level"] <= level]
    res = res.loc[res["path"].str.contains("|".join(dirinclude), na=False)]
    if direxclude:
        res = res.loc[~res["path"].str.contains("|".join(direxclude), na=True)]
    res = res.loc[res["file"].str.contains("|".join(fileinclude), na=False)]
    if fileexclude:
        res = res.loc[~res["file"].str.contains("|".join(fileexclude), na=True)]
    return res.reset_index(drop=True)

# 根據檔名更新 log DataFrame 的指定欄位
def logfromfolder(path, fileinclude, fileexclude, direxclude, dirinclude, log, fillval, avoid=[]):
    pathdf = PathWalk_df(path, fileinclude=fileinclude, fileexclude=fileexclude,
                         direxclude=direxclude, dirinclude=dirinclude)
    log = log.replace({'succeed': 'wait'})
    for name in pathdf['file']:
        col = name.split('_')[0]
        ind = name.split('_')[1].split('.')[0]
        if col in log and ind in log.index:
            if log.loc[ind, col] in avoid:
                continue
            else:
                log.loc[ind, col] = fillval
        else:
            log.loc[ind, col] = fillval
            print("已新增{},{}".format(ind, col))
    return log

# 傳回指定資料夾中最新的檔案修改時間
def datatime_lastest(path=r"", fileexclude=[]):
    file_path = PathWalk_df(path, fileexclude=fileexclude)['path']
    mtime_series = file_path.apply(lambda x: datetime.fromtimestamp(getmtime(x)))
    return max(mtime_series)

# ✅ 回傳檔案/資料夾的屬性與時間資訊（整合 filename 功能）
def sweep_path(path: str) -> pd.Series:
    """
    回傳指定檔案或資料夾路徑的基本屬性與時間資訊。
    """
    p = Path(path).expanduser().resolve()
    info = {
        'path': str(p),
        'exists': p.exists(),
        'is_file': p.is_file(),
        'is_dir': p.is_dir(),
        'name': p.name,
        'parent': str(p.parent),
        'suffix': p.suffix,
        'created_time': datetime.fromtimestamp(p.stat().st_ctime) if p.exists() else None,
        'modified_time': datetime.fromtimestamp(p.stat().st_mtime) if p.exists() else None,
        'accessed_time': datetime.fromtimestamp(p.stat().st_atime) if p.exists() else None,
    }
    return pd.Series(info)

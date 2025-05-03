# -*- coding: utf-8 -*-
"""
File Utility Toolkit
Consolidated utilities for file management, metadata access, serialization, and logging.
"""

from pathlib import Path
import pandas as pd
from datetime import datetime
import pickle
from os import makedirs, walk, remove
from os.path import abspath, join, getmtime


def runninginfo():
    """Print the current execution time and source file (if available)."""
    t = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        file = __file__
    except NameError:
        file = ""
    print(f"在{t}/n執行{file}")
    return {"Time": t, "File": file}


def picklesave(data, path):
    """Save Python object to a pickle file."""
    makedirs(Path(path).parent, exist_ok=True)
    with open(path, 'wb') as f:
        pickle.dump(data, f)


def pickleload(path):
    """Load Python object from a pickle file."""
    with open(path, 'rb') as f:
        return pickle.load(f)


def independentfilename(root, mark="_duplicated", count=1):
    """Return a non-conflicting filename by appending a counter suffix."""
    p = Path(root)
    if not p.exists():
        return str(p)
    base, ext = p.stem, p.suffix
    if mark in base:
        parts = base.split(mark)
        base = parts[0]
        if len(parts) > 1 and parts[1].isdigit():
            count = int(parts[1]) + 1
    newname = f"{base}{mark}{count}{ext}"
    return independentfilename(p.with_name(newname))


def pathlevel(left, right):
    """Return how many directory levels right is below left."""
    left, right = Path(left).resolve(), Path(right).resolve()
    try:
        return len(right.relative_to(left).parts)
    except ValueError:
        return None


def _get_path_stat(p: Path):
    """Extract file/directory timestamps or return None values."""
    stat = p.stat() if p.exists() else None
    return {
        "created_time": datetime.fromtimestamp(stat.st_ctime) if stat else None,
        "modified_time": datetime.fromtimestamp(stat.st_mtime) if stat else None,
        "accessed_time": datetime.fromtimestamp(stat.st_atime) if stat else None
    }


def _list_files(path, file_filter=None):
    """Internal utility to list all files recursively under a path."""
    base = Path(path)
    for p in base.rglob("*"):
        if p.is_file() and (file_filter is None or file_filter(p)):
            yield p


def PathWalk_df(path, dirinclude=[], direxclude=[], fileexclude=[], fileinclude=[], level=None):
    """Return DataFrame of all files in a directory, filtered by pattern."""
    def pathlevel(left, right):
        left, right = Path(left).resolve(), Path(right).resolve()
        try:
            return len(right.relative_to(left).parts)
        except ValueError:
            return None

    rows = []
    for p in _list_files(path):
        rel = str(p.relative_to(path))
        rows.append((p.name, str(p), pathlevel(path, p)))

    df = pd.DataFrame(rows, columns=["file", "path", "level"])
    if level is not None:
        df = df[df["level"] <= level]
    if dirinclude:
        df = df[df["path"].str.contains("|".join(dirinclude), na=False)]
    if direxclude:
        df = df[~df["path"].str.contains("|".join(direxclude), na=False)]
    if fileinclude:
        df = df[df["file"].str.contains("|".join(fileinclude), na=False)]
    if fileexclude:
        df = df[~df["file"].str.contains("|".join(fileexclude), na=False)]
    return df.reset_index(drop=True)


def sweep_path(path: str) -> pd.Series:
    """Return path metadata: existence, type, timestamps, and components."""
    p = Path(path).expanduser().resolve()
    return pd.Series({
        'path': str(p),
        'exists': p.exists(),
        'is_file': p.is_file(),
        'is_dir': p.is_dir(),
        'name': p.name,
        'parent': str(p.parent),
        'suffix': p.suffix,
        **_get_path_stat(p)
    })


def logfromfolder(path, fileinclude, fileexclude, direxclude, dirinclude, log, fillval, avoid=[]):
    """Update a log DataFrame based on presence of files in directory."""
    df = PathWalk_df(path, fileinclude=fileinclude, fileexclude=fileexclude,
                     direxclude=direxclude, dirinclude=dirinclude)
    log = log.replace({'succeed': 'wait'})
    for name in df["file"]:
        parts = name.split('_')
        if len(parts) < 2: continue
        col, ind = parts[0], parts[1].split('.')[0]
        if col in log and ind in log.index:
            if log.loc[ind, col] in avoid:
                continue
        log.loc[ind, col] = fillval
    return log


def logmaker(write_dt, data_dt, log=pd.Series(dtype='object'), period=None, index=None):
    """Compose a logging Series with optional period granularity."""
    if period == "month":
        period = str(data_dt).rsplit("-", 1)[0]
    elif period == "year":
        period = str(data_dt.year)
    elif period == "day":
        period = data_dt
    base = pd.Series({
        "write_dt": write_dt,
        "data_dt": data_dt,
        "period": period,
        "index": index
    }, dtype='object')
    return pd.concat([base, log], axis=1).dropna(how="any", axis=1)

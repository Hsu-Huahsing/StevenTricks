from pathlib import Path
import pandas as pd
from datetime import datetime

class PathSweeper:
    """
    提供檔案與資料夾的基本資訊與時間屬性分析工具。
    支援轉為 pandas Series 以利追蹤與資料儲存。
    """

    def __init__(self, path: str):
        # 將傳入的路徑轉為標準化、絕對路徑（支援 ~）
        self.path = Path(path).expanduser().resolve()
        # 是否存在於系統中
        self.exists = self.path.exists()
        # 是否為檔案
        self.is_file = self.path.is_file()
        # 是否為資料夾
        self.is_dir = self.path.is_dir()

    @property
    def name(self):
        # 傳回檔案或資料夾名稱（不含路徑）
        return self.path.name

    @property
    def parent(self):
        # 傳回上層資料夾路徑
        return str(self.path.parent)

    @property
    def suffix(self):
        # 傳回副檔名（若有）
        return self.path.suffix

    @property
    def modified_time(self):
        # 檔案最後修改時間
        return self._get_time('stat().st_mtime')

    @property
    def created_time(self):
        # 檔案建立時間（Windows: 實際建立時間；Linux: 最後狀態改變時間）
        return self._get_time('stat().st_ctime')

    @property
    def accessed_time(self):
        # 檔案最近被讀取的時間
        return self._get_time('stat().st_atime')

    def _get_time(self, attr: str):
        # 使用 eval 擷取指定的檔案屬性時間戳，轉為 datetime
        try:
            return datetime.fromtimestamp(eval(f'self.path.{attr}'))
        except Exception:
            return None

    def report(self) -> pd.Series:
        # 匯總所有屬性為 pandas Series，方便串接其他資料處理
        return pd.Series({
            'path': str(self.path),
            'exists': self.exists,
            'is_file': self.is_file,
            'is_dir': self.is_dir,
            'name': self.name,
            'parent': self.parent,
            'suffix': self.suffix,
            'created_time': self.created_time,
            'modified_time': self.modified_time,
            'accessed_time': self.accessed_time,
        })


if __name__ == '__main__':
    pass
from pathlib import Path
import pandas as pd
from datetime import datetime

class PathSweeper:
    def __init__(self, path: str):
        self.path = Path(path).expanduser().resolve()
        self.exists = self.path.exists()
        self.is_file = self.path.is_file()
        self.is_dir = self.path.is_dir()

    @property
    def name(self):
        return self.path.name

    @property
    def parent(self):
        return str(self.path.parent)

    @property
    def suffix(self):
        return self.path.suffix

    @property
    def modified_time(self):
        return self._get_time('stat().st_mtime')

    @property
    def created_time(self):
        return self._get_time('stat().st_ctime')

    @property
    def accessed_time(self):
        return self._get_time('stat().st_atime')

    def _get_time(self, attr: str):
        try:
            return datetime.fromtimestamp(eval(f'self.path.{attr}'))
        except Exception:
            return None

    def report(self) -> pd.Series:
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
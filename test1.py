from StevenTricks.dbsqlite import readsql_iter
import pandas as pd
import numpy as np
df_iter = readsql_iter(dbpath=r'/Users/stevenhsu/Library/Mobile Documents/com~apple~CloudDocs/warehouse/ActualPrice/不動產買賣')

# df = pd.concat(df_iter)

df = pd.read_excel(r'/Users/stevenhsu/Library/Mobile Documents/com~apple~CloudDocs/warehouse/ActualPrice/used/a_lvr_land_a.xls')

df1 = df.loc[df[['交易年月日', '建築完成年月']].isnull(), :]

def strtodate(x):
    # 0820412轉成1993-04-12
    if pd.isna(x) is True:
        return

    if 7>=len(x) >= 6:
        x = x.zfill(7)
    else:
        return

    d = x[-2:]
    m = x[-4:-2]
    y = str(int(x[:-4])+1911)

    res = pd.to_datetime("-".join([y,m,d]), errors="coerce")

    if pd.isna(res) is True:
        return
    else:
        return res.strftime("%Y/%m/%d")

d = df[['交易年月日','建築完成年月']]
c = df[['交易年月日','建築完成年月']].map(lambda x: strtodate(x))

d.isna().sum()
c.isna().sum()



b='0891211'
d=b[-2:]
m=b[-4:-2]
y=str(int(b[:-4])+1911)

r='/'.join([y,m,d])
a = pd.to_datetime(r, errors="coerce", format="%Y/%m/%d")
type(a.strftime("%Y/%m/%d"))
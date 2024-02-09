from StevenTricks.dbsqlite import readsql_iter
import pandas as pd
import numpy as np
df_iter = readsql_iter(dbpath=r'/Users/stevenhsu/Library/Mobile Documents/com~apple~CloudDocs/warehouse/ActualPrice/不動產買賣')

# df = pd.concat(df_iter)

df = pd.read_excel(r'/Users/stevenhsu/Library/Mobile Documents/com~apple~CloudDocs/warehouse/ActualPrice/used/a_lvr_land_a.xls')

df1 = df.loc[df[['交易年月日', '建築完成年月']].isnull(), :]




aa = df[['鄉鎮市區','土地位置建物門牌']]
a = df[['鄉鎮市區','土地位置建物門牌']].values
aa['土地位置建物門牌'] = pd.Series([_[1].replace(_[0],'') for _ in a ])





d.isna().sum()
c.isna().sum()



b='0891211'
d=b[-2:]
m=b[-4:-2]
y=str(int(b[:-4])+1911)

r='/'.join([y,m,d])
a = pd.to_datetime(r, errors="coerce", format="%Y/%m/%d")
type(a.strftime("%Y/%m/%d"))
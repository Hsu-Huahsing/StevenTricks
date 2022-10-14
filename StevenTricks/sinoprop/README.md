# Sinoprop
##### For the Sinopac Property analysis.
## Abbreviation & Terms
|Abbr.|O.|CH.|
|-|-|-|
|AP|ActualPrice|實價登錄|
|FC|Foreclosure|法院拍賣|
|GH|GreenHouse|綠建築|
||Agent|房屋仲介|
||Examin|審查|

## A Simple Example
----------------
    >>python -m sinoprop 
**sinoprop/__main__.py will be executed.**
##### Then, couple of options show in the terminal. FC crawler, Agent crawler and AP_price, Valuer, GH data import.

![This is a alt text.](/static/1.JPG "This is a sample of code above.")

##### You could input the number to run specific function.
    >>python roadname_insert.py
##### Insert the roadname column to the db of valuer or examin.
##### It's a preprocess of comparison procedure.
    >>python AP_examin.py
##### Compare AP_price data with Valuer or Examin data.
## NOTE
##### Most functions get involved with abs path, customed package and local database, so that you can not execute repositories directly.
##### All the data derive from package were stored in sqlite3 (.db). If you would like to build your own database, you should install sqlite3(recommanded) or MySQL.

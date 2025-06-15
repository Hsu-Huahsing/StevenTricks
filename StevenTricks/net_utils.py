headers = {
    "safari14.0": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15",
    "iphone13": "Mozilla/5.0 (iPhone; CPU iPhone OS 13_1_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.1 Mobile/15E148 Safari/604.1",
    "ipod13": "Mozilla/5.0 (iPod; CPU iPhone OS 13_1_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.1 Mobile/15E148 Safari/604.1",
    "ipadmini13": "Mozilla/5.0 (iPad; CPU iPhone OS 13_1_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.1 Mobile/15E148 Safari/604.1",
    "ipad": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.1 Safari/605.1.15",
    "winedge": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36 Edge/16.16299",
    "mac": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.70 Safari/537.36",
    "chromewin": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.70 Safari/537.36",
    "firefoxmac": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:70.0) Gecko/20100101 Firefox/70.0",
    "firefoxwin": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:70.0) Gecko/20100101 Firefox/70.0"
}

import requests as re
from traceback import format_exc
from StevenTricks.net_utils import headers
from StevenTricks.dict_utils import randomitem


def randomheader():
    # 隨機產生header，是一個iter
    while True:
        yield {"User-Agent": randomitem(headers)[1]}


def safereturn(res,packet,jsoncheck=False):
    # 如果狀態碼不正確那也不用檢查json了
    # 不用返回packet是因為packet是dictionary，只要引用這個function，內部修改dictionary外面也會跟著連動，所以不用特地再去賦值
    packet['restatuscode']=res.status_code
    packet['restatuscode']=None
    if res.status_code!=re.codes.ok:
        packet["errormessage"] = '{} != {}'.format(str(res.status_code),str(re.codes.ok))
        return [None]

    if jsoncheck is True:
        try:
            jsontext = res.json()
        except:
            packet["errormessage"]=format_exc()
            return [None]
        return [jsontext]
    else:
        return [None]


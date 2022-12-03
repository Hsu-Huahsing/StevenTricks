from StevenTricks.dictur import findstr
from StevenTricks.warren.conf import collection
from StevenTricks.netGEN import randomheader
from traceback import format_exc
import requests as re
# from sys import path


class Packet:
    def __init__(self):
        # title is the target type of stock
        self.title=None
        self.packet=None

    def set_title(self, title):
        self.title=title
        self.packet=collection[title]

    def packet(self, datemin=None):
        # 產生可以放進request的payload
        # 可以指定要放進去的最小日期(datemin)，不然就用預設的最小日期datemin
        # datemin的格式必須為yyyy-m-d
        DateKeyInPayload=findstr(self.packet['payload'], 'date|Date')
        DateKeyInPayload=DateKeyInPayload[0]

        if datemin is None:
            self.packet['payload'][DateKeyInPayload]=self.packet['date_min']
        else:
            self.packet['payload'][DateKeyInPayload]=datemin
        return self.packet

class Spyder:
    def __int__(self):
        self.title=None

    def request(self,packet):
        res=re.post(url=packet['url'],headers=randomheader(),data=packet['payload'],timeout=60)
        try:
            jsontext = res.json()
        except:
            packet["errormessage"]=format_exc()
            packet['restatuscode']=res.status_code
            return None
        



if __name__ == '__main__':
    pass
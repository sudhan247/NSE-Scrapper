import requests
from functools import partial
from six.moves.urllib.parse import urlparse
from bs4 import BeautifulSoup
import pandas as pd
from collections import *
class URLFetch:
    def __init__(self, url, method='get', json=False, session=None,headers = None, proxy = None):
        self.url = url
        self.method = method
        self.json = json
        if not session:
            self.session = requests.Session()
        else:
            self.session = session
        if headers:
            self.session.headers.update(headers)
        if proxy:
            self.update_proxy(proxy)
        else:
            self.update_proxy('')
    def set_session(self, session):
        self.session = session
        return self
    def get_session(self, session):
        self.session = session
        return self
    def __call__(self, *args, **kwargs):
        u = urlparse(self.url)
        self.session.headers.update({'Host': u.hostname})
        url = self.url%(args)
        if self.method == 'get':
            return self.session.get(url, params=kwargs, proxies = self.proxy )
        elif self.method == 'post':
            if self.json:
                return self.session.post(url, json=kwargs, proxies = self.proxy )
            else:
                return self.session.post(url, data=kwargs, proxies = self.proxy )
    def update_proxy(self, proxy):
        self.proxy = proxy
        self.session.proxies.update(self.proxy)
    def update_headers(self, headers):
        self.session.headers.update(headers)
headers = {
    'Accept': '*/*',
           'Accept-Encoding': 'gzip, deflate, sdch, br',
           'Accept-Language': 'en-GB,en-US;q=0.8,en;q=0.6',
           'Connection': 'keep-alive',
           'Host': 'www1.nseindia.com',
           'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
           'X-Requested-With': 'XMLHttpRequest',
     'Referer': 'https://www1.nseindia.com/products/content/equities/equities/bulk.htm'}
session = requests.Session()
URLFetchSession = partial(URLFetch, session=session,
                          headers=headers)
equity_history_url_full = URLFetchSession(
    url='http://www1.nseindia.com/products/dynaContent/equities/equities/bulkdeals.jsp')
equity_history_url = partial(equity_history_url_full,
                             dataType="DEALS",symbol='',
                             segmentLink=13, dateRange="12month")
res=equity_history_url(fromDate="",toDate="")
soup = BeautifulSoup(res.text,features="html.parser")
csv=soup.find("div", {"id": "csvContentDiv"})
data=[];ind=0
for i in csv.text.split(':')[:-1]:
    rec=[j[1:] for j in i.split('",') ]
    rec[-1]=rec[-1][:-1]
    rec=[j for j in rec]
    if len(rec)==8:
        data.append(rec)
    else:
        ind+=1
        if ind%2:
            spec=rec
        else:
            spec[-1]+=(':'+rec[0])
            spec.extend(rec[1:])
            data.append(spec)
df = pd.DataFrame(data[1:],columns=data[0])
print(df.to_string())

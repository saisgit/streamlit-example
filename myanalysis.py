from datetime import datetime, timedelta
from datetime import date
import time
import pandas as pd
import requests
import numpy as np
import urllib
import re
import yfinance as yf
import pandas_ta as ta

def datetotimestamp(date):
    time_tuple = date.timetuple()
    timestamp = round(time.mktime(time_tuple))
    return timestamp

def timstamptodate(timestamp):
    return datetime.fromtimestamp(timestamp)
  
class Moneycontrol:
  def __init__(self):
    self.headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36'}
    self.session = requests.Session()
    self.session.get("http://moneycontrol.com",headers=self.headers)
  def hist_data(self,stock,mydate,timeframe,countback):
    stock = stock.upper().replace(' ','%20').replace('&','%26')
    start1 = datetime.strptime(mydate, '%d%m%Y').date()
    start =  datetotimestamp(start1 - timedelta(days=50)) #1687833709
    end1 =  start1 + timedelta(days=2) #datetotimestamp(end) #1688198140
    end =  datetotimestamp(end1)
    url = "https://priceapi.moneycontrol.com/techCharts/indianMarket/stock/history?symbol="+stock+"&resolution="+str(timeframe)+"&from="+str(start)+"&to="+str(end)+"&countback="+str(countback)+"&currencyCode=INR"
    resp = self.session.get(url,headers=self.headers).json() #329
    data = pd.DataFrame(resp)
    date = []
    for dt in data['t']:
        date.append({'Datetime':timstamptodate(dt)})
    dt = pd.DataFrame(date)
    df = pd.concat([dt,data['o'],data['h'],data['l'],data['c'],data['v']],axis=1).rename(columns={'o':'Open','h':'High','l':'Low','c':'Close','v':'Volume'})
    return df

def get5minMC(fno,current_day_dmy):
    mydf = pd.DataFrame()
    for stock in fno:
        #tday = datetime.strptime(current_day_dmy, '%d%m%Y').date() + timedelta(days=1)
        #duration = "5m"  # Valid intervals: [1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo]
        #nse = Moneycontrol()
        #df = nse.hist_data(stock,current_day_dmy,'5','900')
        tday = datetime.strptime(current_day_dmy, '%d%m%Y').date() + timedelta(days=1)
        startDay = tday - timedelta(days=50)
        duration = "5m"  # Valid intervals: [1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo]
        try:
            proxyServer = urllib.request.getproxies()['http']
        except KeyError:
            proxyServer = ""
        df = yf.download(
            tickers=stock + ".NS",  start=startDay, end=tday,
            #period=period,
            interval=duration,
            proxy=proxyServer,
            progress=False,
            timeout=10
        )
        pd.set_option('display.max_columns', None)
        df['symbol'] = stock
        df.reset_index(inplace=True)
        df.set_index(pd.DatetimeIndex(df["Datetime"]).tz_localize(None), inplace=True)
        #df['adx'] = ta.adx(df['High'], df['Low'], df['Close'],length=14)['ADX_14']
        df.ta.bbands(close='Close', length=50, std=2, append=True) #BBL_50_2  BBM_50_2  BBU_50_2
        #print(df)
        df['bb5mdiff'] = round(((df['BBU_50_2.0'].astype(float)-df['BBL_50_2.0'].astype(float))/df['BBU_50_2.0'].astype(float))*100,2)
        df['BBU_5min'] = df['BBU_50_2.0'].astype(float)
        df['BBL_5min'] = df['BBL_50_2.0'].astype(float)
        df = df[df['Datetime'].astype(str).str.contains(str(datetime.strptime(current_day_dmy, '%d%m%Y').date().strftime("%Y-%m-%d")))]
        df = df.tail(1)
        mydf = pd.concat([mydf,df])
        mydf = mydf[['symbol','bb5mdiff','BBU_5min','BBL_5min','Open','Close','High','Low']]
        mydf.reset_index(inplace=True)
    return round(mydf, 2)[['symbol','bb5mdiff','BBU_5min','BBL_5min','Open','Close','High','Low']]

def hourPivots(fno,current_day_dmy):
    mydf = pd.DataFrame()
    for stock in fno:
        tday = datetime.strptime(current_day_dmy, '%d%m%Y').date() + timedelta(days=1)
        nse = Moneycontrol()
        df = nse.hist_data(stock,current_day_dmy,'1W','3')
        pd.set_option('display.max_columns', None)
        df['symbol'] = stock
        df.reset_index(inplace=True)
        df = df[df['Datetime'].dt.date<=tday]
        if datetime.strptime(current_day_dmy, '%d%m%Y').date().weekday() >= 4:
            full = df.tail(3)
        else:
            full = df.tail(3).head(2)
        full['camR4_hour'] = (full.Close.astype(float)) + (((full.High.astype(float) - full.Low.astype(float))/2)*float(1.1))
        full['camS4_hour'] = (full.Close.astype(float)) - (((full.High.astype(float) - full.Low.astype(float))/2)*float(1.1))
        full['pp_hour'] = (full.High.astype(float) + full.Low.astype(float) + full.Close.astype(float))/3
        full['bc'] = round((full.High.astype(float) + full.Low.astype(float))/2,2)
        full['tc'] = round((full.pp_hour.astype(float) - full.bc.astype(float))+full.pp_hour.astype(float),2)
        full['cpr'] = round((abs(full.tc.astype(float)-full.bc.astype(float))/full.bc.astype(float))*100,2)
        full['r1_hour'] = round((2 * full.pp_hour.astype(float)) - full.Low.astype(float),2)
        full['s1_hour'] = round((2 * full.pp_hour.astype(float)) - full.High.astype(float),2)
        full['pr_dist_hr'] = round(((full['pp_hour'].astype(float)-full['r1_hour'].astype(float))/full['pp_hour'].astype(float))*100,2)
        full['ps_dist_hr'] = round(((full['pp_hour'].astype(float)-full['s1_hour'].astype(float))/full['pp_hour'].astype(float))*100,2)
        full = full.tail(1)
        mydf = pd.concat([mydf,full])
    return round(mydf,2).loc[:,['symbol','pp_hour','r1_hour','s1_hour','pr_dist_hr','ps_dist_hr']]

def get15minMC(fno,current_day_dmy,flag):
    mydf = pd.DataFrame()
    flag = ""
    for stock in fno:
        tday = datetime.strptime(current_day_dmy, '%d%m%Y').date() + timedelta(days=1)
        startDay = tday - timedelta(days=50)
        if flag == 'Y':
            nse1 = Moneycontrol()
            #df = nse1.hist_data_sector(stock,current_day_dmy,'15m')
            df = nse1.hist_data(stock,current_day_dmy,'15','3000')
        else:
            duration = '15m'  # Valid intervals: [1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo]
            try:
                proxyServer = urllib.request.getproxies()['http']
            except KeyError:
                proxyServer = ""
            df = yf.download(
                tickers=stock+".NS",  start=startDay, end=tday,
                #period=period,
                interval=duration,
                proxy=proxyServer,
                progress=False,
                timeout=10
            )
        pd.set_option('display.max_columns', None)
        df['symbol'] = stock
        df.reset_index(inplace=True)
        df.set_index(pd.DatetimeIndex(df["Datetime"]).tz_localize(None), inplace=True)
        df.ta.bbands(close='Close', length=50, std=2, append=True)
        #print(df)
        df.ta.vwap(append=True)
        df.ta.ema(length=50, append=True)
        df['bbands15m'] = round(((df['BBU_50_2.0'].astype(float)-df['BBL_50_2.0'].astype(float))/df['BBU_50_2.0'].astype(float))*100,2)
        df['BBU_50_15m'] = df['BBU_50_2.0'].astype(float)
        df['BBL_50_15m'] = df['BBL_50_2.0'].astype(float)
        df['SMA_50_15m'] = df['BBM_50_2.0'].astype(float)
        df['vwap'] = df['VWAP_D']
        df['EMA50_15m'] = df['EMA_50']
        df['ema50vwap'] = round((abs(df['EMA50_15m'].astype(float)-df['vwap'].astype(float))/df['EMA50_15m'].astype(float))*100,2)
        df['ev'] = np.where(((df.vwap.astype(float) >= df.EMA50_15m.astype(float))), "up","down")
        df = df[df['Datetime'].astype(str).str.contains(str(datetime.strptime(current_day_dmy, '%d%m%Y').date().strftime("%Y-%m-%d")))]
        #print(df)
        df = df.tail(1)
        mydf = pd.concat([mydf,df])
        mydf = mydf[['symbol','bbands15m','BBU_50_15m','BBL_50_15m','SMA_50_15m','vwap','EMA50_15m','ema50vwap','ev']]
        mydf = round(mydf, 2)
        mydf.reset_index(inplace=True)
    return mydf.loc[:, ['symbol','bbands15m','BBU_50_15m','BBL_50_15m','SMA_50_15m','vwap','EMA50_15m','ema50vwap','ev']]

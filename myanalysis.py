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
from tradingview_ta import TA_Handler, Interval, Exchange

def datetotimestamp(date):
    time_tuple = date.timetuple()
    timestamp = round(time.mktime(time_tuple))
    return timestamp

def timstamptodate(timestamp):
    return datetime.fromtimestamp(timestamp)

# def sendtelegram(message):
#     TOKEN = '5649614993:AAFqKCPokaGKxdKlQitcbFIBSV_73KXmhpg'
#     #chat_id = '646254361' #sai
#     chat_id = -1001727416442 #channel
#     #chat_id = -1001936050171 #group
#     message = message.replace('&','_')
#     url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}&text={message}&parse_mode=html"
#     print(requests.get(url).json())

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
        full['r2_hour'] = (full.pp_hour.astype(float)) + (full.High.astype(float) - full.Low.astype(float))
        full['s2_hour'] = (full.pp_hour.astype(float)) - (full.High.astype(float) - full.Low.astype(float))
        #full['r3_hour'] = (full.High.astype(float)) + (2*(full.pp_hour.astype(float) - full.Low.astype(float)))
        full['pp_hour_prev'] = full['pp_hour'].shift(1)
        full['r1_hour_prev'] = full['r1_hour'].shift(1)
        full['s1_hour_prev'] = full['s1_hour'].shift(1)
        full['r2_hour_prev'] = full['r2_hour'].shift(1)
        full['s2_hour_prev'] = full['s2_hour'].shift(1)
        full['pr_dist_hr'] = round(((full['pp_hour'].astype(float)-full['r1_hour'].astype(float))/full['pp_hour'].astype(float))*100,2)
        full['ps_dist_hr'] = round(((full['pp_hour'].astype(float)-full['s1_hour'].astype(float))/full['pp_hour'].astype(float))*100,2)
        full = full.tail(1)
        mydf = pd.concat([mydf,full])
    return round(mydf,2).loc[:,['symbol','pp_hour','r1_hour','r2_hour','s1_hour','s2_hour','pr_dist_hr','ps_dist_hr','pp_hour_prev', 'r1_hour_prev', 's1_hour_prev', 'r2_hour_prev', 's2_hour_prev']]

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
        dff = df[df['Datetime'].astype(str).str.contains(str(datetime.strptime(current_day_dmy, '%d%m%Y').date().strftime("%Y-%m-%d")))]
        df['bb15mHead'] = dff['bbands15m'].iloc[0]
        df['bbsqz'] = np.where((df.bb15mHead.astype(float) >=df['bbands15m'].astype(float)), "sqz","")
        df['sqzpct'] = np.where((df.bbsqz =='sqz'), round((df['bb15mHead'].astype(float)-df['bbands15m'].astype(float))/df['bb15mHead'].astype(float),2),"")
        df['bbsqz'] = df['bbsqz'] +"("+df['sqzpct'].astype(str)+")"
        df = df[df['Datetime'].astype(str).str.contains(str(datetime.strptime(current_day_dmy, '%d%m%Y').date().strftime("%Y-%m-%d")))]
        #print(df)
        df = df.tail(1)
        mydf = pd.concat([mydf,df])
        mydf = mydf[['symbol','bbands15m','BBU_50_15m','BBL_50_15m','SMA_50_15m','vwap','EMA50_15m','ema50vwap','ev','bbsqz']]
        mydf = round(mydf, 2)
        mydf.reset_index(inplace=True)
    return mydf.loc[:, ['symbol','bbands15m','BBU_50_15m','BBL_50_15m','SMA_50_15m','vwap','EMA50_15m','ema50vwap','ev','bbsqz']]

# def get1hourMC_bkp(fno,current_day_dmy):
#     mydf = pd.DataFrame()
#     for stock in fno:
#         tday = datetime.strptime(current_day_dmy, '%d%m%Y').date() + timedelta(days=1)
#         nse = Moneycontrol()
#         df = nse.hist_data(stock,current_day_dmy,'60','3000')
#         pd.set_option('display.max_columns', None)
#         df['symbol'] = stock
#         df.reset_index(inplace=True)
#         df.set_index(pd.DatetimeIndex(df["Datetime"]).tz_localize(None), inplace=True)
#         #df['d_adx'] = ta.adx(df['High'], df['Low'], df['Close'],length=14)['ADX_14']
#         df.ta.ema(length=20, append=True)
#         df.ta.sma(length=20, append=True)
#         df.ta.bbands(close='Close', length=50, std=2, append=True) #BBL_50_2  BBM_50_2  BBU_50_2
#         df.ta.supertrend(period=7, multiplier=3, append=True)
#         df = df.rename(columns={"SUPERT_7_3.0":"hour_st"})
#         df['bbdiff1hr'] = round(((df['BBU_50_2.0'].astype(float)-df['BBL_50_2.0'].astype(float))/df['BBU_50_2.0'].astype(float))*100,2)
#         df['BBU_1hr'] = df['BBU_50_2.0'].astype(float)
#         df['BBL_1hr'] = df['BBL_50_2.0'].astype(float)
#         df['SMA_50'] = df['BBM_50_2.0'].astype(float)
#         df['SMA_20_1hr'] = df['SMA_20'].astype(float)
#         ##df['cross'] = np.where((df.Close.astype(float) >= df['BBU_50_2.0'].astype(float)) & (df.Close_1day_ago.astype(float) <= df['BBU_50_2_1day_ago'].astype(float)), "uband","na")
#         voldf = df[(df['Datetime'].dt.date >= datetime.strptime(current_day_dmy, '%d%m%Y').date() - timedelta(days=15)) & (df['Datetime'].dt.date <= datetime.strptime(current_day_dmy, '%d%m%Y').date())]
#         voldf['cross'] = np.where((voldf.Close.astype(float) <= voldf['BBL_50_2.0'].astype(float)), "lband",np.where((voldf.Close.astype(float) >= voldf['BBU_50_2.0'].astype(float)), "uband","na"))
#         allvols = list(set(voldf['cross'].tail(14).tolist()))
#         if('uband' in allvols): 
#             df['bb_crs'] = 'ub'
#         elif 'lband' in allvols:
#             df['bb_crs'] = 'lb'
#         else:
#             df['bb_crs'] = ''
#         df = df[df['Datetime'].astype(str).str.contains(str(datetime.strptime(current_day_dmy, '%d%m%Y').date().strftime("%Y-%m-%d")))]
#         df = df.tail(1)
#         mydf = pd.concat([mydf,df])
#         mydf = mydf.loc[:,['symbol','bbdiff1hr','bb_crs','BBU_1hr','BBL_1hr','hour_st','SMA_50','EMA_20','SMA_20_1hr']]
#         mydf = round(mydf, 2)
#         mydf.reset_index(inplace=True)
#     return mydf.loc[:,['symbol','bbdiff1hr','bb_crs','BBU_1hr','BBL_1hr','hour_st','SMA_50','EMA_20','SMA_20_1hr']]

def get1hourMC(fno,current_day_dmy):
    df = pd.DataFrame()
    for i in fno:
        try:
            df1 = pd.DataFrame({'symbol': [i]})
            ta_stock = re.sub("[^a-zA-Z0-9 \n\.]", "_", i)
            ta_data = TA_Handler(symbol=ta_stock,screener="india",exchange="NSE",interval=Interval.INTERVAL_1_HOUR,).get_analysis()
            df1['hour_st'] = 0
            df1["BBL_1hr"] = ta_data.indicators['BB.lower']
            df1["BBU_1hr"] = ta_data.indicators['BB.upper']
            df1["SMA_50"] = ta_data.indicators['SMA50']
            df1["EMA_20"] = ta_data.indicators['EMA20']
            df1["SMA_20_1hr"] = ta_data.indicators['SMA20']
            df1['bbdiff1hr'] = round(((df1['BBU_1hr'].astype(float)-df1['BBL_1hr'].astype(float))/df1['BBU_1hr'].astype(float))*100,2)
            df1['bb_crs'] = ''
            df = pd.concat([df,df1])
        except:
            print("An exception occurred")
    return round(df[['symbol','bbdiff1hr','bb_crs','BBU_1hr','BBL_1hr','hour_st','SMA_50','EMA_20','SMA_20_1hr']],2)

def get1dayMC(fno,current_day_dmy):
    mydf = pd.DataFrame()
    for stock in fno:
        #tday = datetime.strptime(current_day_dmy, '%d%m%Y').date() + timedelta(days=1)
        #startDay = tday - timedelta(days=130)
        #duration = "1d"  # Valid intervals: [1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo]
        nse = Moneycontrol()
        df = nse.hist_data(stock,current_day_dmy,'1D','300')
        pd.set_option('display.max_columns', None)
        df['symbol'] = stock
        df.reset_index(inplace=True)
        df.set_index(pd.DatetimeIndex(df["Datetime"]).tz_localize(None), inplace=True)
        #df['d_adx'] = ta.adx(df['High'], df['Low'], df['Close'],length=14)['ADX_14']
        df.ta.bbands(close='Close', length=50, std=2, append=True) #BBL_50_2  BBM_50_2  BBU_50_2
        df.ta.supertrend(period=7, multiplier=3, append=True)
        df = df.rename(columns={"SUPERT_7_3.0":"day_st"})
        df['pp'] = (df.High.astype(float) + df.Low.astype(float) + df.Close.astype(float))/3
        df['bc'] = round((df.High.astype(float) + df.Low.astype(float))/2,2)
        df['tc'] = round((df.pp.astype(float) - df.bc.astype(float))+df.pp.astype(float),2)
        df['cpr'] = round((abs(df.tc.astype(float)-df.bc.astype(float))/df.bc.astype(float))*100,2)
        df['yest_cpr'] = df['cpr'].shift(1)
        #df['cpr_churn'] = round(((df.yest_cpr.astype(float)-df.cpr.astype(float))/df.yest_cpr.astype(float))*100,2)
        #df['bbandsdiff'] = round(((df['BBU_50_2.0'].astype(float)-df['BBL_50_2.0'].astype(float))/df['BBU_50_2.0'].astype(float))*100,2)
        voldf = df[(df['Datetime'].dt.date >= datetime.strptime(current_day_dmy, '%d%m%Y').date() - timedelta(days=25)) & (df['Datetime'].dt.date < datetime.strptime(current_day_dmy, '%d%m%Y').date())]
        voldff = voldf['Volume'].rolling(window=10).mean() #.iloc[-15:]
        voldff4d = voldf['Volume'].rolling(window=4).mean() #.iloc[-15:]
        voldf = voldf.join(voldff, rsuffix='_SMA')
        voldf = voldf.join(voldff4d, rsuffix='_SMA4d')
        voldf['smavol'] = np.where((voldf['Volume'].astype(float) > voldf['Volume_SMA'].astype(float)), "Y", np.nan) #smavol = list (voldf [ ' smavol ' ] . tail (5) . count () ) 
        voldf['dsmavol'] = np.where((voldf['Volume'].astype(float) > voldf['Volume_SMA'].astype(float)) & (voldf['Close'].astype(float) >= voldf['Open'].astype(float)), "YY", np.nan)
        #print (voldf) 
        voldf = voldf.iloc[-5:]
        smamorevol = len(list(voldf[voldf[ "dsmavol"]=='YY']["smavol"].to_list()))
        smavol = len(list(voldf[voldf[ "smavol"]=='Y']["smavol"].to_list()))
        df['Volume_SMA4d'] = voldf['Volume_SMA4d'].astype(float).iloc[-1]
        df['Volume_2'] = df['Volume'].shift(1)
        df['shock'] = round(df['Volume_2'].astype(float)/df['Volume_SMA4d'].astype(float),2)
        df['todayshock'] = round(df['Volume'].astype(float)/df['Volume_SMA4d'].astype(float),2)
        if(smamorevol>0): 
            df['doubledayvol'] = str(smamorevol) +'gg'
        else:
            df['doubledayvol'] = ''
        if(smavol>0): 
            df['dayvol'] = str(smavol) +'D'
        else:
            df['dayvol'] = ''
        df['BBU_1day'] = df['BBU_50_2.0'].astype(float)
        df['BBL_1day'] = df['BBL_50_2.0'].astype(float)
        df['dayvol'] = df['doubledayvol'] + df['dayvol']
        df = df[df['Datetime'].astype(str).str.contains(str(datetime.strptime(current_day_dmy, '%d%m%Y').date().strftime("%Y-%m-%d")))]
        df = df.head(1)
        #print(df)
#         df = df.tail(1)
        mydf = pd.concat([mydf,df])
        mydf = mydf.loc[:,['symbol','dayvol','BBU_1day','BBL_1day','day_st','shock','todayshock']]
        mydf = round(mydf, 2)
        mydf.reset_index(inplace=True)
    return mydf.loc[:,['symbol','dayvol','BBU_1day','BBL_1day','day_st','shock','todayshock']]


def getDayPivots(fno,current_day_dmy):
    mydf = pd.DataFrame()
    for stock in fno:
        tday = datetime.strptime(current_day_dmy, '%d%m%Y').date() + timedelta(days=1)
        startDay = tday - timedelta(days=100)
        duration = "1mo"  # Valid intervals: [1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo]
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
        df = df[df['Date'].dt.date<=tday]
        ##full = df.tail(3).head(2)
        # today = datetime.strptime(current_day_dmy, '%d%m%Y').date() - timedelta(days=1)
        # bussiness_days_rng =pd.date_range(startDay, today, freq='BM')[-1].date()
        # if (today ==  bussiness_days_rng):
            # full = df.tail(3)
        # else:
            # full = df.tail(3).head(2)
        full = df.tail(3).head(2)
        #df = df[df['Date'].astype(str).str.contains(str(working_days[-1].strftime("%Y-%m-%d")))]
        #full = df.head(1)
        full['pp_day'] = (full.High.astype(float) + full.Low.astype(float) + full.Close.astype(float))/3
        full['r1_day'] = round((2 * full.pp_day.astype(float)) - full.Low.astype(float),2)
        full['s1_day'] = round((2 * full.pp_day.astype(float)) - full.High.astype(float),2)
        full['r2_day'] = (full.pp_day.astype(float)) + (full.High.astype(float) - full.Low.astype(float))
        full['s2_day'] = (full.pp_day.astype(float)) - (full.High.astype(float) - full.Low.astype(float))
        
        full['pp_day_prev'] = full['pp_day'].shift(1)
        full['r1_day_prev'] = full['r1_day'].shift(1)
        full['s1_day_prev'] = full['s1_day'].shift(1)
        full['r2_day_prev'] = full['r2_day'].shift(1)
        full['s2_day_prev'] = full['s2_day'].shift(1)
        #print(full)
        full = full.tail(1)
        mydf = pd.concat([mydf,full])
    return round(mydf,2).loc[:,['symbol','pp_day','r1_day','r2_day','s1_day','s2_day','pp_day_prev', 'r1_day_prev', 's1_day_prev', 'r2_day_prev', 's2_day_prev']]

def getsectors():
    data = [
    ['SUNTV','MEDIA'],
    ['ZEEL','MEDIA'],
    ['PVRINOX','MEDIA'],
    ['MOTHERSON','AUTO'],
    ['TATAMOTORS','AUTO'],
    ['HEROMOTOCO','AUTO'],
    ['ASHOKLEY','AUTO'],
    ['BALKRISIND','AUTO'],
    ['EICHERMOT','AUTO'],
    ['BHARATFORG','AUTO'],
    ['TVSMOTOR','AUTO'],
    ['M&M','AUTO'],
    ['BAJAJ-AUTO','AUTO'],
    ['BOSCHLTD','AUTO'],
    ['MRF','AUTO'],
    ['EXIDEIND','AUTO'],
    ['MARUTI','AUTO'],
    ['APOLLOTYRE','AUTO'],
    ['ESCORTS','AUTO'],
    ['HINDPETRO','OILnGAS'],
    ['IGL','OILnGAS'],
    ['MGL','OILnGAS'],
    ['GAIL','OILnGAS'],
    ['ONGC','OILnGAS'],
    ['GUJGASLTD','OILnGAS'],
    ['BPCL','OILnGAS'],
    ['PETRONET','OILnGAS'],
    ['IOC','OILnGAS'],
    ['RELIANCE','OILnGAS'],
    ['BSOFT','IT'],
    ['COFORGE','IT'],
    ['IRCTC','IT'],
    ['NAUKRI','IT'],
    ['PERSISTENT','IT'],
    ['MPHASIS','IT'],
    ['LTIM','IT'],
    ['TECHM','IT'],
    ['INFY','IT'],
    ['OFSS','IT'],
    ['HCLTECH','IT'],
    ['WIPRO','IT'],
    ['TCS','IT'],
    ['HINDCOPPER','METAL'],
    ['NATIONALUM','METAL'],
    ['JINDALSTEL','METAL'],
    ['SAIL','METAL'],
    ['VEDL','METAL'],
    ['NMDC','METAL'],
    ['HINDALCO','METAL'],
    ['TATASTEEL','METAL'],
    ['COALINDIA','METAL'],
    ['JSWSTEEL','METAL'],
    ['ADANIENT','METAL'],
    ['METROPOLIS','PHARMA'],
    ['GLENMARK','PHARMA'],
    ['LALPATHLAB','PHARMA'],
    ['GRANULES','PHARMA'],
    ['LAURUSLABS','PHARMA'],
    ['AUROPHARMA','PHARMA'],
    ['BIOCON','PHARMA'],
    ['DIVISLAB','PHARMA'],
    ['APOLLOHOSP','PHARMA'],
    ['LUPIN','PHARMA'],
    ['SYNGENE','PHARMA'],
    ['IPCALAB','PHARMA'],
    ['ZYDUSLIFE','PHARMA'],
    ['CIPLA','PHARMA'],
    ['ALKEM','PHARMA'],
    ['TORNTPHARM','PHARMA'],
    ['ABBOTINDIA','PHARMA'],
    ['DRREDDY','PHARMA'],
    ['SUNPHARMA','PHARMA'],
    ['DELTACORP','REALTY'],
    ['GODREJPROP','REALTY'],
    ['DLF','REALTY'],
    ['OBEROIRLTY','REALTY'],
    ['INDHOTEL','REALTY'],
    ['SBILIFE','FINANCE'],
    ['MANAPPURAM','FINANCE'],
    ['MCX','FINANCE'],
    ['PEL','FINANCE'],
    ['PFC','FINANCE'],
    ['RECLTD','FINANCE'],
    ['M&MFIN','FINANCE'],
    ['L&TFH','FINANCE'],
    ['CANFINHOME','FINANCE'],
    ['SHRIRAMFIN','FINANCE'],
    ['IDFC','FINANCE'],
    ['ABCAPITAL','FINANCE'],
    ['CHOLAFIN','FINANCE'],
    ['MFSL','FINANCE'],
    ['LICHSGFIN','FINANCE'],
    ['HDFCAMC','FINANCE'],
    ['ICICIPRULI','FINANCE'],
    ['MUTHOOTFIN','FINANCE'],
    ['BAJFINANCE','FINANCE'],
    ['HDFCLIFE','FINANCE'],
    ['BAJAJFINSV','FINANCE'],
    ['SBICARD','FINANCE'],
    ['ICICIGI','FINANCE'],
    ['IBULHSGFIN','FINANCE'],
    ['BALRAMCHIN','FMCG'],
    ['INDIAMART','FMCG'],
    ['JUBLFOOD','FMCG'],
    ['MCDOWELL-N','FMCG'],
    ['BERGEPAINT','FMCG'],
    ['GODREJCP','FMCG'],
    ['UBL','FMCG'],
    ['MARICO','FMCG'],
    ['TATACONSUM','FMCG'],
    ['COLPAL','FMCG'],
    ['PIDILITIND','FMCG'],
    ['ASIANPAINT','FMCG'],
    ['ITC','FMCG'],
    ['DABUR','FMCG'],
    ['BRITANNIA','FMCG'],
    ['NESTLEIND','FMCG'],
    ['HINDUNILVR','FMCG'],
    ['RBLBANK','BANK'],
    ['PNB','BANK'],
    ['CANBK','BANK'],
    ['CUB','BANK'],
    ['BANKBARODA','BANK'],
    ['BANDHANBNK','BANK'],
    ['IDFCFIRSTB','BANK'],
    ['AUBANK','BANK'],
    ['INDUSINDBK','BANK'],
    ['FEDERALBNK','BANK'],
    ['SBIN','BANK'],
    ['AXISBANK','BANK'],
    ['KOTAKBANK','BANK'],
    ['ICICIBANK','BANK'],
    ['HDFCBANK','BANK'],
    ['VOLTAS','CONSUMER DURABLES'],
    ['HAVELLS','CONSUMER DURABLES'],
    ['CROMPTON','CONSUMER DURABLES'],
    ['DIXON','CONSUMER DURABLES'],
    ['TITAN','CONSUMER DURABLES'],
    ['BATAINDIA','CONSUMER DURABLES'],
    ['HAL','CONSUMER DURABLES'],
    ['LTTS','CONSUMER DURABLES'],
    ['ABB','CONSUMER DURABLES'],
    ['BEL','CONSUMER DURABLES'],
    ['POLYCAB','CONSUMER DURABLES'],
    ['ASTRAL','CONSUMER DURABLES'],
    ['BHEL','CONSUMER DURABLES'],
    ['CUMMINSIND','CONSUMER DURABLES'],
    ['SIEMENS','CONSUMER DURABLES'],
    ['LT','CONSUMER DURABLES'],
    ['GNFC','CHEMICALS'],
    ['CHAMBLFERT','CHEMICALS'],
    ['AARTIIND','CHEMICALS'],
    ['NAVINFLUOR','CHEMICALS'],
    ['DEEPAKNTR','CHEMICALS'],
    ['PIIND','CHEMICALS'],
    ['TATACHEM','CHEMICALS'],
    ['ATUL','CHEMICALS'],
    ['UPL','CHEMICALS'],
    ['COROMANDEL','CHEMICALS'],
    ['INDIACEM','CONSTRUCTION'],
    ['AMBUJACEM','CONSTRUCTION'],
    ['ACC','CONSTRUCTION'],
    ['DALBHARAT','CONSTRUCTION'],
    ['JKCEMENT','CONSTRUCTION'],
    ['RAMCOCEM','CONSTRUCTION'],
    ['SHREECEM','CONSTRUCTION'],
    ['ULTRACEMCO','CONSTRUCTION'],
    ['GRASIM','CONSTRUCTION'],
    ['IDEA','TELECOM'],
    ['INDUSTOWER','TELECOM'],
    ['TATACOMM','TELECOM'], ['BHARTIARTL','TELECOM'],
    ['ADANIPORTS','INFRA'],
    ['GMRINFRA','INFRA'],
    ['INDIGO','INFRA'],
    ['CONCOR','INFRA'],
    ['IEX','ENERGY'],
    ['TATAPOWER','ENERGY'],
    ['NTPC','ENERGY'],
    ['POWERGRID','ENERGY'],
    ['TRENT','TEXTILES'],
    ['SRF','TEXTILES'],
    ['PAGEIND','TEXTILES'],
    ['ABFRL','TEXTILES']
    ]
    df = pd.DataFrame(data, columns=['symbol', 'sector'])
    return df
 

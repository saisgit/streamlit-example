import streamlit as st
import time
st.title("Sectors")
import yfinance as yf
import pandas_ta as ta
import pandas as pd
import requests
import numpy as np
import urllib
from datetime import datetime, timedelta
from datetime import date
import sys
from streamlit_gsheets import GSheetsConnection

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
      
  def hist_data_sector(self,stock,mydate,tf,symbol_list=False):
    stock = stock.upper().replace(' ','%20').replace('&','%26')
    start1 = datetime.strptime(mydate, '%d%m%Y').date()
    start =  datetotimestamp(start1 - timedelta(days=50)) #1687833709
    end1 =  start1 + timedelta(days=2) #datetotimestamp(end) #1688198140
    end =  datetotimestamp(end1)
    dic = {'^NSEI':'in%3BNSX','^NSEBANK':'in%3Bnbx','^CNXREALTY':'in%3Bcrl'
           ,'^CNXPSUBANK':'in%3Bcuk','^CNXENERGY':'in%3Bcgy','NIFTY_FIN_SERVICE.NS':'in%3Bcnxf','^CNXFMCG':'in%3Bcfm',
          '^CNXINFRA':'in%3Bcfr','^CNXPHARMA':'in%3Bcpr','NIFTY_HEALTHCARE.NS':'mc%3Bhelcar','NIFTY_OIL_AND_GAS.NS':'mc%3Boilgas'
          ,'^CNXMETAL':'in%3BCNXM','^CNXAUTO':'in%3Bcnxa','^CNXMEDIA':'in%3Bcnmx','^CNXIT':'in%3Bcnit','NIFTY_CONSR_DURBL.NS':'mc%3Bcondur'}
    name = dic[stock]
    if tf == 'D':
        url = "https://priceapi.moneycontrol.com//techCharts/indianMarket/index/history?symbol="+name+"&resolution=1D&from="+str(start)+"&to="+str(end)+"&countback=100&currencyCode=INR"
    elif tf == '5m':
        url = "https://priceapi.moneycontrol.com//techCharts/indianMarket/index/history?symbol="+name+"&resolution=5&from="+str(start)+"&to="+str(end)+"&countback=30000&currencyCode=INR"
    elif tf == '15m':
        url = "https://priceapi.moneycontrol.com//techCharts/indianMarket/index/history?symbol="+name+"&resolution=15&from="+str(start)+"&to="+str(end)+"&countback=30000&currencyCode=INR"
    else:
        url = ""
    resp = self.session.get(url,headers=self.headers).json() #329
    data = pd.DataFrame(resp)
    date = []
    for dt in data['t']:
        date.append({'Datetime':timstamptodate(dt)})
    dt = pd.DataFrame(date)
    df = pd.concat([dt,data['o'],data['h'],data['l'],data['c'],data['v']],axis=1).rename(columns={'o':'Open','h':'High','l':'Low','c':'Close','v':'Volume'})
    #df.ta.ema(length=5, append=True)
    #df = df[df['Datetime'].astype(str).str.contains(str(start1.strftime("%Y-%m-%d")))]
    #df = df.head(1)
    return df
def sector15m(fno,current_day_dmy,flag):
    mydf = pd.DataFrame()
    for stock in fno:
        #print(stock)
        tday = datetime.strptime(current_day_dmy, '%d%m%Y').date() + timedelta(days=1)
        startDay = tday - timedelta(days=50)
        if flag == 'Y':
            nse1 = Moneycontrol()
            df = nse1.hist_data_sector(stock,current_day_dmy,'15m')
        else:
            duration = '15m'  # Valid intervals: [1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo]
            try:
                proxyServer = urllib.request.getproxies()['http']
            except KeyError:
                proxyServer = ""
            df = yf.download(
                tickers=stock,  start=startDay, end=tday,
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
        df['bbands15m'] = round(((df['BBU_50_2.0'].astype(float)-df['BBL_50_2.0'].astype(float))/df['BBU_50_2.0'].astype(float))*100,2)
        df['BBU_50_15m'] = df['BBU_50_2.0'].astype(float)
        df['BBL_50_15m'] = df['BBL_50_2.0'].astype(float)
        dff = df[df['Datetime'].astype(str).str.contains(str(datetime.strptime(current_day_dmy, '%d%m%Y').date().strftime("%Y-%m-%d")))]
        df['bb15mHead'] = dff['bbands15m'].iloc[0]
        df['bbsqz'] = np.where((df.bb15mHead.astype(float) >=df['bbands15m'].astype(float)), "squez","")
        df['sqzpct'] = np.where((df.bbsqz =='squez'), round((df['bb15mHead'].astype(float)-df['bbands15m'].astype(float))/df['bb15mHead'].astype(float),2),"")
        df['crs15'] = np.where((df.Close.astype(float) <= df['BBL_50_2.0'].astype(float)), "lb_15",np.where((df.Close.astype(float) >= df['BBU_50_2.0'].astype(float)), "ub_15","na"))
        df['bbsqz'] = df['bbsqz'] +"("+df['sqzpct'].astype(str)+")"
        #print(df)
        df = df[df['Datetime'].astype(str).str.contains(str(datetime.strptime(current_day_dmy, '%d%m%Y').date().strftime("%Y-%m-%d")))]
        df = df.tail(1)
        mydf = pd.concat([mydf,df])
        mydf = mydf[['symbol','bbands15m','BBU_50_15m','BBL_50_15m','bbsqz','crs15']]
        mydf = round(mydf, 2)
        mydf.reset_index(inplace=True)
    return mydf.loc[:, ['symbol','bbands15m','BBU_50_15m','BBL_50_15m','bbsqz','crs15']]

def daySector(fno,current_day_dmy):
    mydf = pd.DataFrame()
    for stock in fno:
        #print(stock)
        tday = datetime.strptime(current_day_dmy, '%d%m%Y').date() + timedelta(days=1)
        startDay = tday - timedelta(days=3)
        duration = '1d'  # Valid intervals: [1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo]
        nse = Moneycontrol()
        df = nse.hist_data_sector(stock,current_day_dmy,'D')
        pd.set_option('display.max_columns', None)
        df['symbol'] = stock
        df.reset_index(inplace=True)
        df = df[df['Datetime'].astype(str).str.contains(str(datetime.strptime(current_day_dmy, '%d%m%Y').date().strftime("%Y-%m-%d")))]
        df['Yesthigh_price'] = df['High']
        df['Yestlow_price'] = df['Low']
        df['Yestclose_price'] = df['Close']
        df['pp'] = (df.Yesthigh_price.astype(float) + df.Yestlow_price.astype(float) + df.Yestclose_price.astype(float))/3
        df['r1'] = (2 * df.pp.astype(float)) - df.Yestlow_price.astype(float)
        df['s1'] = (2 * df.pp.astype(float)) - df.Yesthigh_price.astype(float)
        df['bc'] = round((df.Yesthigh_price.astype(float) + df.Yestlow_price.astype(float))/2,2)
        df['tc'] = round((df.pp.astype(float) - df.bc.astype(float))+df.pp.astype(float),2)
        df['cpr'] = round((abs(df.tc.astype(float)-df.bc.astype(float))/df.bc.astype(float))*100,4)
        mydf = pd.concat([mydf,df])
        mydf = mydf.loc[:, ['symbol','Yesthigh_price','Yestlow_price','Yestclose_price','pp','r1','s1','cpr']]
        mydf = round(mydf, 2)
        mydf.reset_index(inplace=True)
    return mydf.loc[:, ['symbol','Yesthigh_price','Yestlow_price','Yestclose_price','pp','r1','s1','cpr']]


def main(previous_day_dmy):
	start = time.time()
	#testdate = sys.argv[1]
	#if len(testdate) != 0:
	#	today = datetime.strptime(testdate, '%d%m%Y').date()
	#else:
	today = datetime.today()
	sectors = ['^NSEI','^NSEBANK','^CNXREALTY','^CNXPSUBANK','^CNXENERGY',
            'NIFTY_FIN_SERVICE.NS','^CNXFMCG','^CNXINFRA','^CNXPHARMA','NIFTY_HEALTHCARE.NS','NIFTY_OIL_AND_GAS.NS','^CNXMETAL',
            '^CNXAUTO','^CNXMEDIA','^CNXIT','NIFTY_CONSR_DURBL.NS']
    	#toDate= today.strftime('%Y-%m-%d %H:%M:%S')
	#end_date = pd.Timestamp(toDate, tz='Asia/Kolkata').date()
	#start_date = end_date - pd.Timedelta(days=6)
	#schedule = nse.schedule(start_date=start_date, end_date=end_date)
	#working_days = schedule.index.date
	#current_day_dmy = working_days[-1].strftime("%d%m%Y")
	# prev_day_dmy = working_days[-1].strftime("%d%m%Y")
	#previous_day_dmy = str(working_days[-2].strftime("%d%m%Y")).lstrip().rstrip()
	
	secday = daySector(sectors,previous_day_dmy)
	sec15min = sector15m(sectors,previous_day_dmy,'')
	fulldf = secday.set_index('symbol').join(sec15min.set_index('symbol'), on='symbol')
	fulldf.reset_index(inplace=True)
	conditions = [
        	(fulldf.symbol =='NIFTY_CONSR_DURBL.NS'), 
		(fulldf.symbol =='NIFTY_FIN_SERVICE.NS'),
		(fulldf.symbol =='NIFTY_FIN_SERVICE.NS'),
		(fulldf.symbol =='^CNXIT'),
		(fulldf.symbol =='^CNXPHARMA'),
        	(fulldf.symbol =='^CNXAUTO'),
		(fulldf.symbol =='NIFTY_HEALTHCARE.NS'),
		(fulldf.symbol =='^CNXFMCG'),
		(fulldf.symbol =='^CNXREALTY'),
		(fulldf.symbol =='^CNXMETAL'),
		(fulldf.symbol =='^NSEBANK'),
		(fulldf.symbol =='^CNXMEDIA'),
        	(fulldf.symbol =='NIFTY_OIL_AND_GAS.NS'),
		(fulldf.symbol =='^CNXPSUBANK'),
	]
	choices = ['NIFTY CONSR DURBL', 'NIFTY FIN SERVICE', 'NIFTY FINSRV25 50', 'NIFTY IT', 'NIFTY PHARMA', 'NIFTY AUTO', 'NIFTY HEALTHCARE', 'NIFTY FMCG', 'NIFTY REALTY', 'NIFTY METAL', 'NIFTY BANK', 'NIFTY MEDIA', 'NIFTY OIL AND GAS', 'NIFTY PSU BANK']
	fulldf['nsymbol'] = np.select(conditions, choices, default='')
	fulldf['date'] = str(previous_day_dmy)
	fulldf = fulldf.loc[:, ['nsymbol','symbol','date','Yesthigh_price','Yestlow_price','Yestclose_price','pp','r1','s1','bbands15m','BBU_50_15m','BBL_50_15m','bbsqz','cpr']]
	fulldf = fulldf.sort_values(by=['bbands15m'], ascending=True)
	end = time.time()
	st.write("Time Taken:{}".format(end - start))
	return fulldf


conn = st.experimental_connection("gsheets", type=GSheetsConnection)
previous_day_dmy = st.text_input('previous_day_dmy', '19042024')
st.write("Enter previous date of Market run date")
if st.button("Get Sector Data"):
	df = main(previous_day_dmy)
	#st.write(df)
	conn.update(worksheet="sectors",data=df)
	st.write("DB updated")

data = conn.read(worksheet="sectors",usecols=list(range(20)),ttl="0").dropna(how="all")
st.dataframe(data)
# button = st.button("start")
# placeholder = st.empty()
# if button:
#     with st.spinner():
#         counter = 0
#         while True:
#             st.write("Waiting...")
#             if placeholder.button("Stop", key=counter): # otherwise streamlit complains that you're creating two of the same widget
#                 break
#             time.sleep(5)
#             counter += 1

# st.write("done")  # in this sample this code never executed

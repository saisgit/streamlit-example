import streamlit as st
import numpy as np
import pandas as pd
import streamlit as st
import requests
from streamlit_gsheets import GSheetsConnection
from myanalysis import hourPivots,get15minMC,get5minMC,get1hourMC,get1dayMC,getDayPivots
from multiprocessing import Pool
import time
st.set_page_config(
  page_title="Multipage App",
  initial_sidebar_state='collapsed',
  layout="wide"
)
padding = 2
st.markdown(f"""
    <style>
         .main .block-container{{
        padding-top: {padding}rem;
        padding-right: {padding}rem;
        padding-left: {padding}rem;
        padding-bottom: {padding}rem;
        }}
     div.stButton > button:first-child {{
        background-color: #578a00;
        padding:1px;
        height:auto;
        font-size:4px;
        color:#ffffff;
    }}
    div.stButton > button:hover {{
        background-color: #00128a;
        color:#ffffff;
        }};
    table {{background-color: #f0f0f0;font-size:5px;}}
    </style>""",
    unsafe_allow_html=True,
)

class NSE():
  pre_market_categories = ['NIFTY 50','Nifty Bank','Emerge','Securities in F&O','Others','All']
  equity_market_categories = []
  holidays_categories = ["Clearing","Trading"]

  def __init__(self):
    self.headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36'}
    self.session = requests.Session()
    self.session.get("http://nseindia.com",headers=self.headers)

  def equity_market_data(self,category,symbol_list=False):
    category = category.upper().replace(' ','%20').replace('&','%26')
    data = self.session.get(f"https://www.nseindia.com/api/equity-stockIndices?index={category}",headers=self.headers).json()["data"]
    df = pd.DataFrame(data)
    df = df.drop(["meta"],axis=1)
    df = df.set_index("symbol",drop=True)
    if symbol_list:
      return list(df.index)
    else:
      return df

  def getbhavcopy(self,dt):
    fnolink_tdy = "https://archives.nseindia.com/products/content/sec_bhavdata_full_{0}.csv".format(dt)
    tdy = pd.read_csv(fnolink_tdy, skipinitialspace=True)
    # ydy = pd.read_csv(fnolink_ydy, skipinitialspace=True)
    tdy = tdy.reset_index(inplace=False)
    # LOGGER.info(tdy.columns)
    tdy.columns = tdy.columns.str.replace(' ', '')
    # LOGGER.info(tdy['SERIES'])
    tdy['SERIES'].str.replace(' ', '')
    tdy['OPEN_PRICE'].astype(str).str.replace(' ', '')
    tdy['HIGH_PRICE'].astype(str).str.replace(' ', '')
    tdy['LOW_PRICE'].astype(str).str.replace(' ', '')
    #tdy['CLOSE_PRICE'].astype(str).str.replace(' ', '')
    tdy = tdy.drop(columns=['CLOSE_PRICE'])
    tdy['LAST_PRICE'].astype(str).str.replace(' ', '')
    tdy = tdy.rename(columns={"LAST_PRICE":"CLOSE_PRICE"})
    tdy = tdy[tdy['SERIES'] == 'EQ']
    tdy = round(tdy, 2)
    return tdy

  def get_vol(previous_day_dmy):
    fnolink_tdy = "https://archives.nseindia.com/archives/nsccl/volt/CMVOLT_{0}.CSV".format(previous_day_dmy)
    # fnolink_tdy = "https://archives.nseindia.com/archives/nsccl/volt/CMVOLT_22022023.CSV"
    tdy = pd.read_csv(fnolink_tdy)
    # tdy = tdy.convert_dtypes(infer_objects=False)
    tdy = tdy.reset_index(inplace=False)
    tdy = tdy[tdy['Symbol'].isin(fno)]
    tdy = tdy.rename(columns={"Current Day Underlying Daily Volatility (E) = Sqrt(0.995*D*D + 0.005*C*C)": "volatile",'Symbol':'symbol'})
    tdy = tdy[['Date','symbol','volatile']]
    tdy["volatile"] = pd.to_numeric(tdy['volatile'],errors='coerce')
    # tdy["volatile"] = tdy["volatile"].str.replace('-', '')#.astype(float)
    tdy['vix'] = tdy['volatile'].multiply(100) #.apply(lambda x: x*100)
    return tdy


def volshock(todayshock):
    if todayshock > 1.4:
        return 'background-color: green'

current_day_dmy = st.text_input('current_day_dmy', 'ddmmyyyy')
previous_day_dmy = st.text_input('previous_day_dmy', 'ddmmyyyy')
dby_day_dmy = st.text_input('dby_day_dmy', 'ddmmyyyy')

def myanalysis(current_day_dmy,previous_day_dmy,dby_day_dmy):
  nse = NSE()
  fno = nse.equity_market_data('Securities in F&O',symbol_list=True)
  n50 = nse.equity_market_data('NIFTY 50',symbol_list=True)
  n50.remove("NIFTY 50")
  df1 = nse.equity_market_data("Securities in F&O")[['open','dayHigh','dayLow','lastPrice','totalTradedVolume','previousClose']].reset_index()
  df1 = df1.rename(columns={"totalTradedVolume": "volume"})
  df1 = round(df1,2)
  

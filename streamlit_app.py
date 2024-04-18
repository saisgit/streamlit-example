import altair as alt
import numpy as np
import pandas as pd
import streamlit as st
import requests
from streamlit_gsheets import GSheetsConnection
st.set_page_config(
  page_title="Multipage App",
  initial_sidebar_state='collapsed',
  layout="wide"
)
#st.sidebar.success("Select pages")
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
      
def add(ss):
  st.write(ss)
  
if st.button('add'):
    result = add("hello")
    st.write('result: %s' % result)
nse = NSE()
high = nse.equity_market_data("Securities in F&O")[['open','dayHigh','dayLow','lastPrice','totalTradedVolume','previousClose']].reset_index()
high = high.rename(columns={"totalTradedVolume": "volume",'lastPrice':'Close'})
high = round(high,2)
#st.write(high)
conn = st.experimental_connection("gsheets", type=GSheetsConnection)
# style
th_props = [
  ('font-size', '8px')
  ]                             
td_props = [
  ('font-size', '10px')
  ]                              
styles = [
  dict(selector="th", props=th_props),
  dict(selector="td", props=td_props)
  ]

col1, col2 = st.columns(2)
with col1:
  st.header("sell")
  data = conn.read(worksheet="Sheet2",ttl="0")
  outputdframe = st.dataframe(data)
  df2=outputdframe.style.set_properties(**{'text-align': 'left'}).set_table_styles(styles)
  st.table(df2)
with col2:
  st.header("buy")
  data = conn.read(worksheet="Sheet2",ttl="0")
  st.dataframe(data)
#data = conn.read(worksheet="Sheet2",ttl="0")

if st.button("update"):
  conn.update(worksheet="Sheet2",data=high)
  st.success("worksheet updated")

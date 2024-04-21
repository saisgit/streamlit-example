import numpy as np
import pandas as pd
import streamlit as st
import requests
from streamlit_gsheets import GSheetsConnection
from myanalysis import hourPivots,get15minMC,get5minMC,get1hourMC,get1dayMC,getDayPivots
from multiprocessing import Pool
import time
import streamlit.components.v1 as components
from pandas.api.types import (
    is_categorical_dtype,
    is_datetime64_any_dtype,
    is_numeric_dtype,
    is_object_dtype,
)
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
def highlight(sig):
    if sig == "P_BUY" or sig == "BBBBBB":
        return 'background-color: green'
    if sig == "P-SELL" or sig == "SSSSSS":
        return 'background-color: red'

@st.cache_data
def get_data():
    conn = st.experimental_connection("gsheets", type=GSheetsConnection)
    data = conn.read(worksheet="Sheet2",usecols=list(range(45)),ttl="0").dropna(how="all")
    df = pd.DataFrame(data)
    return df
	
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
      

nse = NSE()
high = nse.equity_market_data("Securities in F&O")[['open','dayHigh','dayLow','lastPrice','totalTradedVolume','previousClose','pChange']].reset_index()
high = high.rename(columns={"totalTradedVolume": "volume",'lastPrice':'Close'})
high = round(high,2)
#st.write(high)
#conn = st.experimental_connection("gsheets", type=GSheetsConnection)
# style
th_props = [
  ('font-size', '10px')
  ]                             
td_props = [
  ('font-size', '8px')
  ]                              
styles = [
  dict(selector="th", props=th_props),
  dict(selector="td", props=td_props)
  ]
#data = conn.read(worksheet="Sheet2",usecols=list(range(45)),ttl="0").dropna(how="all")
#bbsqueeze = pd.DataFrame(data)
bbsqueeze = get_data()
bbsqueeze = bbsqueeze[bbsqueeze["pp_dist"].isin(["P1","P2"])]
high = high.set_index('symbol').join(bbsqueeze.set_index('symbol'), on='symbol')
high.reset_index(inplace=True)
high['bb15m'] = np.where(((high.Close.astype(float) >= high.BBU_50_15m.astype(float))), "u15",np.where(((high.Close.astype(float) <= high.BBL_50_15m.astype(float))), "lo15",""))
conditions = [
		(high.open.astype(float) <= high.pp_hour.astype(float)) & (high.Close.astype(float) > high.pp_hour.astype(float)),
		(high.open.astype(float) <= high.r1_hour.astype(float)) & (high.Close.astype(float) > high.r1_hour.astype(float)),
		(high.Close.astype(float) >= high.pp_hour.astype(float)) & (high.Close.astype(float) < high.r1_hour.astype(float)),
		(high.Close.astype(float) >= high.r1_hour.astype(float)) & (high.Close.astype(float) < high.r2_hour.astype(float)),
		(high.Close.astype(float) >= high.r2_hour.astype(float)),
		(high.open.astype(float) >= high.pp_hour.astype(float)) & (high.Close.astype(float) < high.pp_hour.astype(float)),
		(high.Close.astype(float) <= high.pp_hour.astype(float)) & (high.Close.astype(float) > high.s1_hour.astype(float)),
		(high.open.astype(float) >= high.s1_hour.astype(float)) & (high.Close.astype(float) < high.s1_hour.astype(float)),
		(high.Close.astype(float) <= high.s1_hour.astype(float)) & (high.Close.astype(float) > high.s2_hour.astype(float)),
		(high.Close.astype(float) <= high.s2_hour.astype(float))
		]
choices = ['crsPP','crsR1','pp-R1', 'R1-R2', '>R2','crsblwPP','pp-S1','crsS1','S1-S2','<S2']
high['hourPvt'] = np.select(conditions, choices, default='')
high['gaps'] = np.where(((high.open.astype(float) >= high.Yesthigh_price.astype(float)) & (high.open.astype(float) > high.Yestclose_price.astype(float).mul(1.002))), "GapUp",np.where((high.open.astype(float) <= high.Yestlow_price.astype(float)) & (high.open.astype(float) < high.Yestclose_price.astype(float).mul(0.998)), "GapDown",""))

#high = high[high["pp_dist"].isin(["P1","P2"])]
#st.write(high)
high['signal'] = np.where(((high.hourPvt.isin(["pp-R1","crsPP","crsR1",])) & ((high.Close.astype(float) >= high.BBU_5min.astype(float)))), "BUY",np.where(((high.hourPvt.isin(['crsblwPP','pp-S1','crsS1'])) & (high.Close.astype(float) <= high.BBL_5min.astype(float))), "SELL",""))
#high2 = st.dataframe(filter_dataframe(high))
conditions = [
	(high.Close.astype(float) >= high.BBU_5min.astype(float)) & (high.Close.astype(float) >= high.r1.astype(float)) & (high.bb15m == "u15") & (high.hourPvt.isin(["pp-R1","crsPP","crsR1"])) & (high.gaps == "GapUp"),
	(high.Close.astype(float) >= high.BBU_5min.astype(float)) & (high.Close.astype(float) >= high.r1.astype(float)) & (high.bb15m == "u15") & (high.hourPvt.isin(["pp-R1","crsPP","crsR1"])),
	(high.Close.astype(float) >= high.r1.astype(float)) & (high.gaps == "GapUp"),
        (high.Close.astype(float) <= high.BBL_5min.astype(float)) & (high.Close.astype(float) <= high.s1.astype(float)) & (high.bb15m == "lo15") & (high.hourPvt.isin(['crsblwPP','pp-S1','crsS1'])) & (high.gaps == "GapDown"),
	(high.Close.astype(float) <= high.BBL_5min.astype(float)) & (high.Close.astype(float) <= high.s1.astype(float)) & (high.bb15m == "lo15") & (high.hourPvt.isin(['crsblwPP','pp-S1','crsS1'])),
	(high.Close.astype(float) <= high.s1.astype(float)) & (high.gaps == "GapDown"),
	]
choices = ['BBBBBB','P-BUY','B', 'SSSSSS', 'P-SELL','S']
high['sig'] = np.select(conditions, choices, default='')
modify = st.checkbox("All FNO Stocks")
if modify:
        high = high
else:
	high = high[(high["N50"].str.contains("Y", na=False))]

#high = high.loc[:,['symbol','sig','pChange','hourPvt','sdist','bb15m','bbands15m','sector']]
highB = high[(high["signal"].str.contains("BUY", na=False))]
highS = high[(high["signal"].str.contains("SELL", na=False))]
#highB = high2[high2["signal"].astype(str).str.contains("BUY")]
#highS = high2[high2["signal"].astype(str).str.contains("SELL")]
col1, col2 = st.columns(2)
with col1:
  st.header("sell")
  #data = conn.read(worksheet="Sheet2",usecols=list(range(7)),ttl="0").dropna(how="all")
  #df = pd.DataFrame(data).head(10)
  #df = df.reset_index(drop=True)
  #df2=df.style.set_properties(**{'text-align': 'left'}).set_table_styles(styles)
  #st.table(df2)
  #s = st.dataframe(filter_nifty(highS))
  s = highS.loc[:,['symbol','sig','pChange','hourPvt','sdist','bb15m','bbands15m','sector']]
  #s = s.style.applymap(highlight, subset=['sig'])
  st.dataframe(s)
  #st.dataframe(s.format({"f": "{:.2f}"}))
with col2:
  st.header("buy")
  # data = conn.read(worksheet="Sheet2",usecols=list(range(7)),ttl="0").dropna(how="all")
  # df = pd.DataFrame(data).head(10)
  # df2=df.style.set_properties(**{'text-align': 'left'}).set_table_styles(styles)
  # st.table(df2)
  #b = st.dataframe(filter_nifty(highB))
  b = highB.loc[:,['symbol','sig','pChange','hourPvt','sdist','bb15m','bbands15m','sector']]
  #st.dataframe(filter_dataframe(s))
  #b = b.style.applymap(highlight, subset=['sig'])
  st.dataframe(b)
  #st.dataframe(b.format({"f": "{:.2f}"}))

if st.button("refresh"):
  st.rerun()


col1, col2,col3, col4,col5, col6,col7 = st.columns(7)
	
with col1:
	st.subheader("fmcg")
	high1 = high.loc[(high["sector"].str.contains("FMCG", na=False))]
	hh = high1.loc[:,['symbol','sig','pChange','hourPvt']]
	st.dataframe(hh)
	
with col2:
	st.subheader("pharma")
	high1 = high.loc[(high["sector"].str.contains("PHARMA", na=False))]
	hh = high1.loc[:,['symbol','sig','pChange','hourPvt']]
	st.dataframe(hh)
	
with col3:
	st.subheader("media")
	high1 = high.loc[(high["sector"].str.contains("MEDIA", na=False))]
	hh = high1.loc[:,['symbol','sig','pChange','hourPvt']]
	st.dataframe(hh)
	
with col4:
	st.subheader("IT")
	high1 = high.loc[(high["sector"].str.contains("IT", na=False))]
	hh = high1.loc[:,['symbol','sig','pChange','hourPvt']]
	st.dataframe(hh)
	
with col5:
	st.subheader("oilgas")
	high1 = high.loc[(high["sector"].str.contains("OILnGAS", na=False))]
	hh = high1.loc[:,['symbol','sig','pChange','hourPvt']]
	st.dataframe(hh)
	
with col6:
	st.subheader("infra")
	high1 = high.loc[(high["sector"].str.contains("INFRA", na=False))]
	hh = high1.loc[:,['symbol','sig','pChange','hourPvt']]
	st.dataframe(hh)
	
with col7:
	st.subheader("energy")
	high1 = high.loc[(high["sector"].str.contains("ENERGY", na=False))]
	hh = high1.loc[:,['symbol','sig','pChange','hourPvt']]
	st.dataframe(hh)
#if st.button("update"):
#  conn.update(worksheet="Sheet2",data=high)
#  st.success("worksheet updated")

# title = st.text_input('Stocks', 'ABB')
# titles = list(title.split(","))

# if st.button("5mins_data"):
#   df = get5minMC(titles,"16042024")
#   st.write(df)
  
# if st.button("day_data"):
#   df = get1dayMC(titles,"16042024")
#   st.write(df)
  
# if st.button("hourpivot"):
#   df = hourPivots(titles,"16042024")
#   st.write(df)
  
# if st.button("15mins_data"):
#   df = get15minMC(titles,"16042024","")
#   st.write(df)

# fno = nse.equity_market_data('Securities in F&O',symbol_list=True)
# if st.button("all pivots"):
#   if __name__ ==  '__main__':
#       start = time.time()
#       end = time.time()
#       st.write("Time Taken:{}".format(end - start))
#       ipsplits =4
#       allsplits3 = np.array_split(fno, ipsplits)
#       tasks3 = map(lambda x:(allsplits3[x],"16042024"),range(0,ipsplits)) # current_day_dmy change to previousday when running for before day
#       with Pool(ipsplits) as executor:
#         results = executor.starmap(hourPivots,iterable=tasks3)
#       pivotDF = pd.concat(results)
#       #df = hourPivots(fno,"16042024")
#       end = time.time()
#       st.write(pivotDF)
#       st.write("Time Taken:{}".format(end - start))
    

#if st.button("all pivots"):
#  df = hourPivots(fno,"16042024")
#  st.write(df)


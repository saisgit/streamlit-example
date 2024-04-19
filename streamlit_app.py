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

def filter_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a UI on top of a dataframe to let viewers filter columns

    Args:
        df (pd.DataFrame): Original dataframe

    Returns:
        pd.DataFrame: Filtered dataframe
    """
    modify = st.checkbox("Add filters")

    if not modify:
        return df

    df = df.copy()

    # Try to convert datetimes into a standard format (datetime, no timezone)
    for col in df.columns:
        if is_object_dtype(df[col]):
            try:
                df[col] = pd.to_datetime(df[col])
            except Exception:
                pass

        if is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.tz_localize(None)

    modification_container = st.container()

    with modification_container:
        to_filter_columns = st.multiselect("Filter dataframe on", df.columns)
        for column in to_filter_columns:
            left, right = st.columns((1, 20))
            # Treat columns with < 10 unique values as categorical
            if is_categorical_dtype(df[column]) or df[column].nunique() < 10:
                user_cat_input = right.multiselect(
                    f"Values for {column}",
                    df[column].unique(),
                    default=list(df[column].unique()),
                )
                df = df[df[column].isin(user_cat_input)]
            elif is_numeric_dtype(df[column]):
                _min = float(df[column].min())
                _max = float(df[column].max())
                step = (_max - _min) / 100
                user_num_input = right.slider(
                    f"Values for {column}",
                    min_value=_min,
                    max_value=_max,
                    value=(_min, _max),
                    step=step,
                )
                df = df[df[column].between(*user_num_input)]
            elif is_datetime64_any_dtype(df[column]):
                user_date_input = right.date_input(
                    f"Values for {column}",
                    value=(
                        df[column].min(),
                        df[column].max(),
                    ),
                )
                if len(user_date_input) == 2:
                    user_date_input = tuple(map(pd.to_datetime, user_date_input))
                    start_date, end_date = user_date_input
                    df = df.loc[df[column].between(start_date, end_date)]
            else:
                user_text_input = right.text_input(
                    f"Substring or regex in {column}",
                )
                if user_text_input:
                    df = df[df[column].astype(str).str.contains(user_text_input)]

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
conn = st.experimental_connection("gsheets", type=GSheetsConnection)
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
data = conn.read(worksheet="Sheet2",usecols=list(range(35)),ttl="0").dropna(how="all")
bbsqueeze = pd.DataFrame(data)
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
high = high[high["pp_dist"].isin(["P1","P2"])]
#st.write(high)
high['signal'] = np.where(((high.hourPvt.isin(["pp-R1","crsPP","crsR1",])) & ((high.Close.astype(float) >= high.BBU_5min.astype(float)))), "BUY",np.where(((high.hourPvt.isin(['crsblwPP','pp-S1','crsS1'])) & (high.Close.astype(float) <= high.BBL_5min.astype(float))), "SELL",""))
high = st.dataframe(filter_dataframe(high))
#highB = high[(high["signal"].str.contains("BUY", na=False))]
#highS = high[(high["signal"].str.contains("SELL", na=False))]
highB = high[high["signal"].astype(str).str.contains("BUY")]
highS = high[high["signal"].astype(str).str.contains("SELL")]
col1, col2 = st.columns(2)
with col1:
  st.header("sell")
  #data = conn.read(worksheet="Sheet2",usecols=list(range(7)),ttl="0").dropna(how="all")
  #df = pd.DataFrame(data).head(10)
  #df = df.reset_index(drop=True)
  #df2=df.style.set_properties(**{'text-align': 'left'}).set_table_styles(styles)
  #st.table(df2)
  s = highS.loc[:,['symbol','signal','pChange','hourPvt','sdist','bb15m','bbands15m']]
  st.write(s)
with col2:
  st.header("buy")
  # data = conn.read(worksheet="Sheet2",usecols=list(range(7)),ttl="0").dropna(how="all")
  # df = pd.DataFrame(data).head(10)
  # df2=df.style.set_properties(**{'text-align': 'left'}).set_table_styles(styles)
  # st.table(df2)
  b = highB.loc[:,['symbol','signal','pChange','hourPvt','sdist','bb15m','bbands15m']]
  st.write(b)

if st.button("refresh"):
  st.rerun()
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


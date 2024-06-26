import streamlit as st
import numpy as np
import pandas as pd
import streamlit as st
import requests
from streamlit_gsheets import GSheetsConnection
from myanalysis import hourPivots,get15minMC,get5minMC,get1hourMC,get1dayMC,getDayPivots,getsectors
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



def myanalysis(current_day_dmy,previous_day_dmy,dby_day_dmy,fnostocks):
    nse = NSE()
    start = time.time()
    fno = nse.equity_market_data(str(fnostocks),symbol_list=True) #'Securities in F&O'
    fno.remove("LTF")
    n50 = nse.equity_market_data('NIFTY 50',symbol_list=True)
    n50.remove("NIFTY 50")
    st.write(fno)
    #df1 = nse.equity_market_data("Securities in F&O")[['open','dayHigh','dayLow','lastPrice','totalTradedVolume','previousClose']].reset_index()
    #df1 = df1.rename(columns={"totalTradedVolume": "volume"})
    df1 = nse.getbhavcopy(previous_day_dmy)
    df1 = df1.rename(columns={"SYMBOL":"symbol","OPEN_PRICE": "open","HIGH_PRICE": "dayHigh","LOW_PRICE": "dayLow","CLOSE_PRICE": "lastPrice","TTL_TRD_QNTY": "volume","PREV_CLOSE":"previousClose"})
    df1 = df1[df1['symbol'].isin(fno)]
    df1 = round(df1,2)
    all = fno
    all.remove("LTF")
    ydy = nse.getbhavcopy(previous_day_dmy)
    ydy = ydy[ydy['SYMBOL'].isin(all)]
    ydy.columns = ydy.columns.str.lower()
    ydy = ydy.add_prefix('Yest')
    dbdy = nse.getbhavcopy(dby_day_dmy)
    dbdy = dbdy[dbdy['SYMBOL'].isin(all)]
    dbdy.columns = dbdy.columns.str.lower()
    dbdy = dbdy.add_prefix('dby')
    full = df1.set_index('symbol').join(ydy.set_index('Yestsymbol'), on='symbol')
    full.reset_index(inplace=True)
    full = full.set_index('symbol').join(dbdy.set_index('dbysymbol'), on='symbol')
    full.reset_index(inplace=True)
    #volt = nse.get_vol(previous_day_dmy)
    #full = full.set_index('symbol').join(volt.set_index('symbol'), on='symbol')
    #full.reset_index(inplace=True)
    full['prevdata'] = np.where((full.dayHigh.astype(float) >  full.Yesthigh_price.astype(float)), "brkPrevHigh",np.where((full.dayLow.astype(float) <  full.Yestlow_price.astype(float)), "brkPrevlow","")) 
    full['pp'] = (full.Yesthigh_price.astype(float) + full.Yestlow_price.astype(float) + full.Yestclose_price.astype(float))/3
    full['yest_pp'] = (full.dbyhigh_price.astype(float) + full.dbylow_price.astype(float) + full.Yestprev_close.astype(float))/3
    full['bc'] = round((full.Yesthigh_price.astype(float) + full.Yestlow_price.astype(float))/2,2)
    full['yest_bc'] = round((full.dbyhigh_price.astype(float) + full.dbylow_price.astype(float))/2,2)
    full['tc'] = round((full.pp.astype(float) - full.bc.astype(float))+full.pp.astype(float),2)
    full['yest_tc'] = round((full.yest_pp.astype(float) - full.yest_bc.astype(float))+full.yest_pp.astype(float),2)
    full['r1'] = round((2 * full.pp.astype(float)) - full.Yestlow_price.astype(float),2)
    full['s1'] = round((2 * full.pp.astype(float)) - full.Yesthigh_price.astype(float),2)
    full['r2'] = (full.pp.astype(float)) + (full.Yesthigh_price.astype(float) - full.Yestlow_price.astype(float))
    full['yest_r2'] = (full.yest_pp.astype(float)) + (full.dbyhigh_price.astype(float) - full.dbylow_price.astype(float))
    full['s2'] = (full.pp.astype(float)) - (full.Yesthigh_price.astype(float) - full.Yestlow_price.astype(float))
    full['camR4'] = (full.Yestclose_price.astype(float)) + (((full.Yesthigh_price.astype(float) - full.Yestlow_price.astype(float))/2)*float(1.1))
    full['camS4'] = (full.Yestclose_price.astype(float)) - (((full.Yesthigh_price.astype(float) - full.Yestlow_price.astype(float))/2)*float(1.1))
    full['camR3'] = (full.previousClose.astype(float)) + (((full.Yesthigh_price.astype(float) - full.Yestlow_price.astype(float))/4)*float(1.1))
    full['camS3'] = (full.previousClose.astype(float)) - (((full.Yesthigh_price.astype(float) - full.Yestlow_price.astype(float))/4)*float(1.1))
    #S3 = C - [1.1 * (H - L) / 4]
    full['YcamR3'] = (full.Yestprev_close.astype(float)) + (((full.dbyhigh_price.astype(float) - full.dbylow_price.astype(float))/4)*float(1.1))
    full['YcamS3'] = (full.Yestprev_close.astype(float)) - (((full.dbyhigh_price.astype(float) - full.dbylow_price.astype(float))/4)*float(1.1))
    full['rdist'] = abs(round(((full.r2.astype(float)-full.r1.astype(float))/full.r2.astype(float))*100,2))
    full['sdist'] = abs(round(((full.s1.astype(float)-full.s2.astype(float))/full.s1.astype(float))*100,2))
    full['cpr'] = round((abs(full.tc.astype(float)-full.bc.astype(float))/full.bc.astype(float))*100,2)
    full['yest_cpr'] = round((abs(full.yest_tc.astype(float)-full.yest_bc.astype(float))/full.yest_bc.astype(float))*100,2)
    #full['gaps'] = np.where(((full.open.astype(float) >= full.Yesthigh_price.astype(float)) & (full.open.astype(float) > full.Yestclose_price.astype(float).mul(1.002))), "GapUp",np.where((full.open.astype(float) <= full.Yestlow_price.astype(float)) & (full.open.astype(float) < full.Yestclose_price.astype(float).mul(0.998)), "GapDown",""))
    full['cams'] = np.where(((full.camR3.astype(float) <= full.YcamR3.astype(float)) & (full.camS3.astype(float) >= full.YcamS3.astype(float))), "CM","")
    full['candle'] = np.where(((full.Yesthigh_price.astype(float) <= full.dbyhigh_price.astype(float)) & (full.Yestlow_price.astype(float) >= full.dbylow_price.astype(float))), "inside",np.where((full.Yestlow_price.astype(float) <= full.dbylow_price.astype(float)) & (full.Yesthigh_price.astype(float) >= full.dbyhigh_price.astype(float)), "outside",""))
    full['candle'] = full['candle'] + full['cpr'].astype(str)
    full['N50'] = np.where(full['symbol'].isin(n50), "Y","")
    fnolen = len(fno)
    if fnolen < 4:
        ips = 2
    elif fnolen >16:
        ips = 4
    else:
        ips = 1
    allsplits = np.array_split(all, ips)
    s = time.time()
    tasks = map(lambda x:(allsplits[x],previous_day_dmy),range(0,ips))
    with Pool(ips) as executor:
      results = executor.starmap(get5minMC,iterable=tasks)
      executor.close()
    bb_df = pd.concat(results)
    bb_df = bb_df.set_index('symbol').join(full.set_index('symbol'), on='symbol') #df1
    bb_df.reset_index(inplace=True)
    bb_df['cls_5m_r2'] = np.where((bb_df.Close.astype(float) <= bb_df['yest_r2'].astype(float)), "<5m_R2","")
    bbs = fno
    if bbs:
        bblen = len(bbs)
        if bblen < 4:
            ipsplits = 1
        elif bblen >16:
            ipsplits = 4
        else:
            ipsplits = 2
        ## GET 15min DATA
        allsplits15 = np.array_split(bbs, ipsplits)
        tasks1 = map(lambda x:(allsplits15[x],previous_day_dmy,'Y'),range(0,ipsplits))
        with Pool(ipsplits) as executor:
                results = executor.starmap(get15minMC,iterable=tasks1)
        my15mDF = pd.concat(results)
        allsplits1 = np.array_split(bbs, ipsplits)
        tasks1 = map(lambda x:(allsplits1[x],previous_day_dmy),range(0,ipsplits))
        with Pool(ipsplits) as executor:
                results = executor.starmap(get1hourMC,iterable=tasks1)
        stDF = pd.concat(results)
        allsplits2 = np.array_split(bbs, ipsplits)
        tasks2 = map(lambda x:(allsplits2[x],previous_day_dmy),range(0,ipsplits))
        with Pool(ipsplits) as executor:
                results = executor.starmap(get1dayMC,iterable=tasks2)
        mDF = pd.concat(results)
        ## GET HOURLY PIVOT DATA
        allsplits3 = np.array_split(bbs, ipsplits)
        tasks3 = map(lambda x:(allsplits3[x],previous_day_dmy),range(0,ipsplits)) # current_day_dmy change to previousday when running for before day
        with Pool(ipsplits) as executor:
                results = executor.starmap(hourPivots,iterable=tasks3)
        pivotDF = pd.concat(results)
        ## GET DAILY PIVOT DATA
        allsplits4 = np.array_split(bbs, ipsplits)
        tasks4 = map(lambda x:(allsplits4[x],current_day_dmy),range(0,ipsplits))
        with Pool(ipsplits) as executor:
                results = executor.starmap(getDayPivots,iterable=tasks4)
        daypivotDF = pd.concat(results)
        e = time.time()
        st.write("Time Taken:{}".format(e - s))
        bb_df1 = bb_df.set_index('symbol').join(stDF.set_index('symbol'), on='symbol')
        bb_df1.reset_index(inplace=True)
        bb_df2 = bb_df1.set_index('symbol').join(mDF.set_index('symbol'), on='symbol')
        bb_df2.reset_index(inplace=True)
        bb_df3 = bb_df2.set_index('symbol').join(pivotDF.set_index('symbol'), on='symbol')
        bb_df3.reset_index(inplace=True)
        bb_df4 = bb_df3.set_index('symbol').join(daypivotDF.set_index('symbol'), on='symbol')
        bb_df4.reset_index(inplace=True)
        bb_df = bb_df4.set_index('symbol').join(my15mDF.set_index('symbol'), on='symbol')
        bb_df.reset_index(inplace=True)
        ## GET HOURLY PIVOTS DATA
        conditions = [
            (bb_df.Open.astype(float) <= bb_df.pp_hour.astype(float)) & (bb_df.Close.astype(float) > bb_df.pp_hour.astype(float)),
            (bb_df.Open.astype(float) <= bb_df.r1_hour.astype(float)) & (bb_df.Close.astype(float) > bb_df.r1_hour.astype(float)),
            (bb_df.Close.astype(float) >= bb_df.pp_hour.astype(float)) & (bb_df.Close.astype(float) < bb_df.r1_hour.astype(float)),
            (bb_df.Close.astype(float) >= bb_df.r1_hour.astype(float)) & (bb_df.Close.astype(float) < bb_df.r2_hour.astype(float)),
            (bb_df.Close.astype(float) >= bb_df.r2_hour.astype(float)),
            (bb_df.Close.astype(float) <= bb_df.pp_hour.astype(float)) & (bb_df.Close.astype(float) > bb_df.s1_hour.astype(float)),
            (bb_df.Close.astype(float) <= bb_df.s1_hour.astype(float)) & (bb_df.Close.astype(float) > bb_df.s2_hour.astype(float)),
            (bb_df.Close.astype(float) <= bb_df.s2_hour.astype(float))
            ]
        choices = ['crsPP','crsR1','pp-R1', 'R1-R2', '>R2','pp-S1','S1-S2','<S2']
        bb_df['hourPvt'] = np.select(conditions, choices, default='')
        ## GET DIALY PIVOT DATA
        conditions = [
            (bb_df.Open.astype(float) <= bb_df.pp_day.astype(float)) & (bb_df.Close.astype(float) > bb_df.pp_day.astype(float)),
            (bb_df.Open.astype(float) <= bb_df.r1_day.astype(float)) & (bb_df.Close.astype(float) > bb_df.r1_day.astype(float)),
            (bb_df.Close.astype(float) >= bb_df.pp_day.astype(float)) & (bb_df.Close.astype(float) < bb_df.r1_day.astype(float)),
            (bb_df.Close.astype(float) >= bb_df.r1_day.astype(float)) & (bb_df.Close.astype(float) < bb_df.r2_day.astype(float)),
            (bb_df.Close.astype(float) >= bb_df.r2_day.astype(float)),
            (bb_df.Close.astype(float) <= bb_df.pp_day.astype(float)) & (bb_df.Close.astype(float) > bb_df.s1_day.astype(float)),
            (bb_df.Close.astype(float) <= bb_df.s1_day.astype(float)) & (bb_df.Close.astype(float) > bb_df.s2_day.astype(float)),
            (bb_df.Close.astype(float) <= bb_df.s2_day.astype(float))
            ]
        choices = ['crsPP','crsR1','pp-R1', 'R1-R2', '>R2','pp-S1','S1-S2','<S2']
        bb_df['dayPvt'] = np.select(conditions, choices, default='')
        ## GET SIGNAL
        conditions = [
            (bb_df.dayPvt.isin(["pp-R1"])) & (bb_df.hourPvt.isin(['crsPP','pp-R1','crsR1'])),
            (bb_df.dayPvt.isin(["crsPP"])) & (bb_df.hourPvt.isin(['crsPP','pp-R1','crsR1'])),
            (bb_df.dayPvt.isin(["crsPP","pp-R1"])) & (bb_df.hourPvt.isin(['R1-R2','pp-S1'])),
            (bb_df.dayPvt.isin(["crsPP","pp-R1"])) & (bb_df.hourPvt.isin(['>R2'])),
            (bb_df.dayPvt == "crsR1") & (bb_df.hourPvt.isin(['crsPP','pp-R1','crsR1'])),
            (bb_df.dayPvt == "crsR1") & (bb_df.hourPvt.isin(['R1-R2','>R2'])),
            (bb_df.dayPvt == "R1-R2") & (bb_df.hourPvt.isin(['crsPP','pp-R1','crsR1'])),
            (bb_df.dayPvt == "R1-R2") & (bb_df.hourPvt.isin(['R1-R2'])),
            (bb_df.hourPvt.isin(['crsPP']) ), #& (bb_df.dayvol.str.contains("g")
            ]
        choices = ['PBUY','BUY','BUY','B', 'BUY', 'B','BUY','B','BUY']
        bb_df['go'] = np.select(conditions, choices, default='')
        bb_df['bb_day_dist'] = round(((bb_df['Close'].astype(float)-bb_df['BBU_1day'].astype(float))/bb_df['Close'].astype(float))*100,2)
        #bb_df['go'] = np.where((bb_df.hourPvt == "crsR1") | (bb_df.hourBB == "crsBBU") | (bb_df.hourST == "crsA_hrST") | (bb_df.dayBB == "crsBBU_D") | (bb_df.dayST == "crsA_dST"), "PBUY",np.where((bb_df.hourPvt == "S1-S2"), "B",""))
        
        bb_df['1hrPP'] = round(((bb_df['Close'].astype(float)-bb_df['pp_hour'].astype(float))/bb_df['Close'].astype(float))*100,2)
        bb_df['SMA_50_15m_d'] = round(((bb_df['Close'].astype(float)-bb_df['SMA_50_15m'].astype(float))/bb_df['Close'].astype(float))*100,2)
        bb_df['SMA_20_1hr_d'] = round(((bb_df['Close'].astype(float)-bb_df['SMA_20_1hr'].astype(float))/bb_df['Close'].astype(float))*100,2)
        bb_df['1hrR1'] = round(((bb_df['Close'].astype(float)-bb_df['r1_hour'].astype(float))/bb_df['Close'].astype(float))*100,2)
        bb_df['1hrS1'] = round(((bb_df['Close'].astype(float)-bb_df['s1_hour'].astype(float))/bb_df['Close'].astype(float))*100,2)
        bb_df['1hrR2'] = round(((bb_df['Close'].astype(float)-bb_df['r2_hour'].astype(float))/bb_df['Close'].astype(float))*100,2)
        bb_df['1hrbb_U'] = round(((bb_df['Close'].astype(float)-bb_df['BBU_1hr'].astype(float))/bb_df['Close'].astype(float))*100,2)
        bb_df['1hrbb_L'] = round(((bb_df['Close'].astype(float)-bb_df['BBL_1hr'].astype(float))/bb_df['Close'].astype(float))*100,2)
        bb_df['EMA_20_dist'] = round((abs(bb_df['Close'].astype(float)-bb_df['EMA_20'].astype(float))/bb_df['Close'].astype(float))*100,2)
        
        bb_df['pp_dist'] = np.where((bb_df['1hrPP'].astype(float) >=float(-0.5)) & (bb_df['1hrPP'].astype(float) <=float(0.5)), "P1",np.where((bb_df['1hrPP'].astype(float) >=float(-1)) & (bb_df['1hrPP'].astype(float) <=float(1)),"P2",np.where((bb_df['1hrPP'].astype(float) >=float(-1.5)) & (bb_df['1hrPP'].astype(float) <=float(1.5)),"P3","")))
        bb_df['bbu_dist'] = np.where((bb_df['1hrbb_U'].astype(float) >=float(-0.5)) & (bb_df['1hrbb_U'].astype(float) <=float(0.5)), "P1",np.where((bb_df['1hrbb_U'].astype(float) >=float(-1)) & (bb_df['1hrbb_U'].astype(float) <=float(1)),"P2",np.where((bb_df['1hrbb_U'].astype(float) >=float(-1.5)) & (bb_df['1hrbb_U'].astype(float) <=float(1.5)),"P3","")))
        bb_df['SMA_50_15m_dist'] = np.where((bb_df['SMA_50_15m_d'].astype(float) >=float(-0.5)) & (bb_df['SMA_50_15m_d'].astype(float) <=float(0.5)), "P1",np.where((bb_df['SMA_50_15m_d'].astype(float) >=float(-1)) & (bb_df['SMA_50_15m_d'].astype(float) <=float(1)),"P2",np.where((bb_df['SMA_50_15m_d'].astype(float) >=float(-1.5)) & (bb_df['SMA_50_15m_d'].astype(float) <=float(1.5)),"P3","")))
        bb_df['SMA_20_1hr_dist'] = np.where((bb_df['SMA_20_1hr_d'].astype(float) >=float(-0.5)) & (bb_df['SMA_20_1hr_d'].astype(float) <=float(0.5)), "P1",np.where((bb_df['SMA_20_1hr_d'].astype(float) >=float(-1)) & (bb_df['SMA_20_1hr_d'].astype(float) <=float(1)),"P2",np.where((bb_df['SMA_20_1hr_d'].astype(float) >=float(-1.5)) & (bb_df['SMA_20_1hr_d'].astype(float) <=float(1.5)),"P3","")))
        bb_df['date'] = str("'")+str(current_day_dmy)+str("'")
        secdf = getsectors()
        bb_df = bb_df.set_index('symbol').join(secdf.set_index('symbol'), on='symbol',how='left')
        bb_df.reset_index(inplace=True)
        fulldf = bb_df.loc[:,['symbol','date','sector','go','pp_dist','SMA_50_15m_dist','SMA_20_1hr_dist','rdist','sdist','bb5mdiff','bbands15m','todayshock','shock','dayvol','bbsqz','hourPvt','dayPvt','bbu_dist','N50','candle','ema50vwap','ev','pr_dist_hr','ps_dist_hr','BBU_5min','BBL_5min','BBU_50_15m','BBL_50_15m','pp_hour','r1_hour','r2_hour','s1_hour','s2_hour','r1','s1','Yesthigh_price','Yestlow_price','Yestclose_price']]
        fulldf = fulldf.drop_duplicates()
        fulldf = fulldf.style.applymap(volshock, subset=['todayshock','shock'])
        end = time.time()
        print("Time Taken:{}".format(end - start))
    return fulldf

current_day_dmy = st.text_input('current_day_dmy', '19042024')
previous_day_dmy = st.text_input('previous_day_dmy', '18042024')
dby_day_dmy = st.text_input('dby_day_dmy', '16042024')
fnostocks = st.radio(
  "Choose n50 or fno",
  ['NIFTY 50', 'Securities in F&O'],
  index=None,
  )
st.write("You selected:", fnostocks)
#st.experimental_user.email
if st.button("Get analysis"):
  if __name__ ==  '__main__':
    df = myanalysis(current_day_dmy,previous_day_dmy,dby_day_dmy,fnostocks)
    #st.write(df)
    conn = st.experimental_connection("gsheets", type=GSheetsConnection)
    conn.update(worksheet="Sheet2",data=df)
    st.success("DB updated")
    
# if st.button("Read Data"):
#   conn = st.experimental_connection("gsheets", type=GSheetsConnection)
#   data = conn.read(worksheet="Sheet2",usecols=list(range(35)),ttl="0").dropna(how="all")
#   st.write(data)
conn = st.experimental_connection("gsheets", type=GSheetsConnection)
data = conn.read(worksheet="Sheet2",usecols=list(range(40)),ttl="0").dropna(how="all")
st.dataframe(filter_dataframe(data))
  

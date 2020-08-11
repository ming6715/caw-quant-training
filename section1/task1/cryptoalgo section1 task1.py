#!/usr/bin/env python
# coding: utf-8

# # Task1 Get hourly candle data from CryptoCompare

# 1. Write a function to download histohour data, parameters:

# In[1]:


import requests
import pandas as pd


# In[2]:


payload = {'fsym': 'BTC', 'tsym': 'USDT', 'start_time':"2017-04-01", 'end_time':"2020-04-01", 'e':'binance'}
r = requests.get('https://min-api.cryptocompare.com/data/v2/histohour?',params=payload)


# In[3]:


print(r.url)


# In[4]:


#return joson data
OHLCV_dct = r.json()['Data']['Data']
OHLCV_df = pd.DataFrame(OHLCV_dct).iloc[:,:-2]


# In[5]:


#format
#change col names
OHLCV_df.rename(columns={'time':'datetime','volumefrom':'baseVolume','volumeto':'volume'},inplace=True)
#change col orders
orders=['close','high','low','open','volume','baseVolume','datetime']
OHLCV_df = OHLCV_df[orders]
#change datetime format
#OHLCV_df.dtypes
OHLCV_df['datetime']=pd.to_datetime(OHLCV_df['datetime'],unit='s')


# In[14]:


OHLCV = OHLCV_df.to_csv(index=False) #no path, return strings
print(OHLCV)


# Optional
# 1. Modularize your code
# write a class for CryptoCompare data api object and put your function into a member function.
# 
# 2. Add one more data endpoint
# write a member function for one more endpoint, e.x. Toplist by Market Cap Full Data. (feel free to choose another one) and put it as another member function.

# In[71]:


#any general ways to flatten the dict in a df?
class CryptoCompare(object):
    def OHLCV(self,fsym,tsym,start_time,end_time,tryConversion='true',e='CCCAGG',aggregate=1,
              aggregatePredictableTimePeriods='true',limit=168, explainPath='false',sign='false',path=None):
        
        payload = {'tryConversion':tryConversion, 'fsym':fsym, 'tsym':tsym, 'e':e, 'aggregate' : aggregate, 
                   'aggregatePredictableTimePeriods':aggregatePredictableTimePeriods, 'limit':limit,
                    'start_time':start_time, 'end_time':end_time, ' explainPath': explainPath, 'sign':sign}
        
        r = requests.get('https://min-api.cryptocompare.com/data/v2/histohour?',params=payload)
        
        #return joson data
        OHLCV_dct = r.json()['Data']['Data']
        OHLCV_df = pd.DataFrame(OHLCV_dct).iloc[:,:-2]
        
        #format
        #change col names
        OHLCV_df.rename(columns={'time':'datetime','volumefrom':'baseVolume','volumeto':'volume'},inplace=True)
        #change col orders
        orders=['close','high','low','open','volume','baseVolume','datetime']
        OHLCV_df = OHLCV_df[orders]
        #change datetime format
        OHLCV_df['datetime']=pd.to_datetime(OHLCV_df['datetime'],unit='s')
        
        OHLCV = OHLCV_df.to_csv(path_or_buf=path,index=False)
        return OHLCV
    
    
    def Top_MarketCap(self,tsym,limit=10,page=0,sign='false',ascending='true',path=None):
        payload = {'limit': limit, 'page': page, 'tsym':tsym, 'ascending':ascending, 'sign':sign}
        r = requests.get('https://min-api.cryptocompare.com/data/top/mktcapfull?',params=payload)
        
        #return joson data
        MarketCap_dct = r.json()['Data']
        MarketCap_df = pd.DataFrame(MarketCap_dct)#flatten CoinInfo
        CoinInfo = MarketCap_df['CoinInfo'].apply(pd.Series)
        #flatten Rating
        Rating = CoinInfo['Rating'].apply(pd.Series)['Weiss'].apply(pd.Series)
        Rating.columns=map(lambda x:'Rating_Weiss_'+str(x),Rating.columns)
        #replace original Rating to the flattened colunms
        i = CoinInfo.columns.tolist().index('Rating')
        new_cols=Rating.columns.tolist()

        for col in range(len(new_cols)-1,-1,-1):
            CoinInfo.insert(i,new_cols[col],Rating.values[:,col])

        CoinInfo = CoinInfo.drop(columns='Rating')

        CoinInfo.columns=map(lambda x:'CoinInfo_'+str(x),CoinInfo.columns)

        #flatten RAW
        RAW = MarketCap_df['RAW'].apply(pd.Series)['USD'].apply(pd.Series)
        RAW.columns=map(lambda x:'RAW_'+str(x),RAW.columns)

        #flatten DISPLAY
        DISPLAY =  MarketCap_df['DISPLAY'].apply(pd.Series)['USD'].apply(pd.Series)
        DISPLAY.columns=map(lambda x:'DISPLAY_'+str(x),DISPLAY.columns)

        MarketCap_df = pd.concat([CoinInfo,RAW,DISPLAY],axis=1)

        MarketCap = MarketCap_df.to_csv(path_or_buf=path,index=False)
        return MarketCap       


# In[72]:


cyp = CryptoCompare() 


# In[73]:


OHLCV_csv = cyp.OHLCV(fsym= 'BTC', tsym= 'USDT', start_time="2017-04-01", end_time="2020-04-01", e='binance')
print(OHLCV_csv)


# In[75]:


Top_MarketCap_csv = cyp.Top_MarketCap(tsym='USD')
Top_MarketCap_csv


# In[ ]:





# In[ ]:





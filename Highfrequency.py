
import pandas as pd
import numpy as np


def Garman_Class_Volatility_estimator(x):
    #input One Subinterval of High Frequency Data
    result=(0.5*np.log(x.max()/x.min())**2)-(2*np.log(2)-1)*np.log(x.head(1).iloc[0,0]/x.tail(1).iloc[0,0])**2
    return result


def Daily_Volaltility(DF_prices,sub_intervals=None):
    """ INPUT: Price Data Dataframe with X amount sub-intervals per day
               Slicing a Dataframe is much faster than getting Series
               N sub-intervals < 0.5* sup(T_day(i))
        OUTPUT Garman Class Daily volatility estimator
    """
    if sub-intervals==None:
        # Using one interval
        Daily_Grouped_Prices=DF_prices.groupby(DF_prices.date)
        result=Daily_Grouped_Prices.apply(Garman_Class_Volatility_estimator(x))
        return result
        #elif sub_intervals >0.5 min(T) to be implemented

def VWAP(Data,price,volume):
    Grouped=Data.groupby(Data.index)

    Weights=Data[volume]/Grouped[volume].transform("sum")
    Weighted_prices=Data[price]*Weights

    result=Weighted_prices.groupby(Data.index).sum()
    return result

def TradeDirection(Data,col_ASKS,col_BIDS,col_price):
    """
    Input Dataframe with Price of trades, Best asks and Best Bids

    Output Dataframe with BUY Column indicating Buy market order :1 or Sell Market order 0 according to Lee and Ready Paper
    """

    Data["Midpoint"]=(Data[col_ASKS]+Data[col_BIDS])/2
    Price=Data[col_price]
    # First Everything is a sell:
    Data["BUY"]=False
    QUOTES_TRADES.loc[QUOTES_TRADES["VWAP"]>QUOTES_TRADES["Midpoint"],"BUY"]=True
    # If price bigger than Midpoint : definitely a buy
    Data.loc[Price>Data["Midpoint"],"BUY"]=True  # Set Trades where P> Midpoint to TRUE

    # IF price equal to midpoint: look for uptick in first difference

    Data["First_Difference"]=Data[col_price]-Data[col_price].shift()
    Data.loc[(Price==Data["Midpoint"])& (Data["First_Difference"]>0),"BUY"]=True


    # If price equal to midpoint and no  in first difference look at second uptick
    Data["Second difference"]= Data[col_price]-Data[col_price].shift(2)

    if Data.loc[(Data[col_price]==Data["Midpoint"]) &(Data["First_Difference"]==0)&(Data["Second difference"]>0) ,"BUY"].any():
        Data.loc[(Data[col_price]==Data["Midpoint"]) &
                 (Data["First_Difference"==0]) &(Data["Second difference"]>0) ,"BUY"]=True
    else:
        pass

    Data.loc[Data["BUY"]==True,"BUY"]=1.
    Data.loc[Data["BUY"]==False,"BUY"]=-1.

    return Data

def support_level(x,col_bids,crash_sellof_amount):
    """
    input x                  :Dask Bag with mapped Json load function
          col_bids           : name of the bids in the json file
          crash_sellof_amount: Amount of bitcoins that needs to be sold to be considered a crash e.g if 40 000 bitcoins are sold price drops so hard that is considered a crash
    """
    dataframe=pd.dataframe(x[col_bids])
    dataframe=dataframe.iloc[::-1]
    dataframe["cumvolume"]=dataframe["amount"].cumsum()
    if dataframe.loc[dataframe["cumvolume"]>=crash_sellof_amount].any().any():
        breach=dataframe[dataframe["cumvolume"]>=crash_sellof_amount]["price"].iloc[0]
        return breach
    else:
        return np.nan
    
    
    
def Aggresive_Orderflow_imbalance(Data,col_buysell,col_price,col_amount,Frequency):
     """
    input Data :Dataframe holding Trade data with price , amount  and a classifier if it is a market buy or sell
          Frequency: The frequency on which the aggresive order flow imbalance should be aggregated. e.g. 4H
    """
    Data=Data.assign(Agressive_OBF=lambda x: x[col_amount]*x[col_buysell])
    Data=Data.assign(Agressive_OBF_dollar=lambda x: x[col_amount]*x[col_buysell]*x[col_price])
    result=Data[["Agressive_OBF","Agressive_OBF_dollar"]].groupby(pd.Grouper(freq=Frequency)).sum()
    return result

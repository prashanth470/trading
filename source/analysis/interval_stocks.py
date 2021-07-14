#------------------ For each time interval units, calculates best and worst stock
import pymongo
import numpy as np
import logging
from kiteconnect import KiteConnect


myclient = pymongo.MongoClient("mongodb://localhost:27017/")
time_heat_map = myclient["core"]["analytics"]["time_heat"]
stocks_for_interval = myclient["core"]["analytics"]["interval_stocks"]
functional_data_col =myclient["core"]["functional"]
indices_col = myclient["core"]["indices"]
# stocks_for_interval.delete_many({})

intervals = ['minute', 'day', '3minute', '5minute', '10minute', '15minute', '30minute', '60minute']
stock_rank_limit = functional_data_col.find({"variable":"rank_limit"},{"_id":0, "values":1 })[0]['values']
price_limit = functional_data_col.find({"variable":"price_limit"},{"_id":0, "values":1 })[0]['values']
interval_counts = functional_data_col.find({"variable":"max_count_per_interval"}, {"_id":0,"values":1})[0]['values']
# print(stock_rank_limit, price_limit, interval_counts)


def interval_level_stocks(time_coll):
    
    for interval in intervals:        
        interval_level_dict = {}
        interval_level_dict['interval'] = interval
        interval_units = time_coll.find({}, {"_id":0})[0][interval].keys()
        for interval_unit in interval_units:
            # print('Processing: interval: ', interval, ', interval unit: ', interval_unit)
            interval_unit_level_dict = {}
            interval_unit_max = {}
            interval_unit_min = {}
            interval_unit_max_returns = 0
            interval_unit_min_returns = 0
            for instruments in time_coll.find({},{"_id":0}):
                try:
                    # rank = indices_col.find({"instrument_token":instruments["instrument_token"]},{"_id":0, "rank":1})[0]['rank']
                    # print(decide_price_limit(instruments["tradingsymbol"]))
                    # if instruments[interval][interval_unit]['count'] == interval_counts[interval] and decide_price_limit(instruments["tradingsymbol"]):
                        # print(interval_unit)
                        # print(instruments[interval][interval_unit]['average_return'], interval_unit,instruments["tradingsymbol"] )
                        if instruments[interval][interval_unit]['average_return'] > interval_unit_max_returns:
                            interval_unit_max_returns = instruments[interval][interval_unit]['average_return']
                            interval_unit_max["tradingsymbol"] = instruments["tradingsymbol"]
                            interval_unit_max["name"] = instruments["name"]
                            interval_unit_max["instrument_token"] = instruments["instrument_token"]
                            interval_unit_max["returns"] = instruments[interval][interval_unit]['average_return']
                        if instruments[interval][interval_unit]['average_return'] < interval_unit_min_returns:
                            interval_unit_min_returns = instruments[interval][interval_unit]['average_return']
                            interval_unit_min["tradingsymbol"] = instruments["tradingsymbol"]
                            interval_unit_min["name"] = instruments["name"]
                            interval_unit_min["instrument_token"] = instruments["instrument_token"]
                            interval_unit_min["returns"] = instruments[interval][interval_unit]['average_return']
                except Exception as e:
                    pass
                
                #print('max: ', interval_unit_max_returns,' min: ', interval_unit_min_returns)
            
            interval_unit_level_dict['max_return'] = interval_unit_max
            interval_unit_level_dict['min_return'] = interval_unit_min
            stocks_for_interval.update_one({"interval":interval},{"$set":{interval_unit:interval_unit_level_dict}})
            # interval_level_dict[interval_unit] = interval_unit_level_dict
            # print(interval_unit_level_dict)
        #print(interval_level_dict)
        # stocks_for_interval.insert_one(interval_level_dict)

# def decide_price_limit(trading_symbol):
#     trading_symbol = 'NSE:'+str(trading_symbol)
#     last_traded_price = kite.quote([trading_symbol])[trading_symbol]['last_price']
#     if last_traded_price >= price_limit:
#         return True
#     return False
            
       

interval_level_stocks(time_heat_map)

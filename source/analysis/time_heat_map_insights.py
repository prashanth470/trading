#--------- Calculates the average maximum and average minimum return time interval zones for each stock

import pymongo
import numpy as np

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
time_heat_map = myclient["core"]["analytics"]["time_heat"]
time_heat_map_insights = myclient["core"]["analytics"]["time_heat"]["time_heat_insights"]
# time_heat_map_insights.delete_many({})
intervals = ['minute', 'day', '3minute', '5minute', '10minute', '15minute', '30minute', '60minute']

def time_insights(time_data_coll):

    for instruments in time_data_coll.find({},{"_id":0}):
        insights_dict = {}
        insights_dict["tradingsymbol"] = instruments["tradingsymbol"]
        insights_dict["name"] = instruments["name"]
        insights_dict["instrument_token"] = instruments["instrument_token"]
        for interval in intervals:
            # print('Processing: instrument- ', instruments["tradingsymbol"], ' interval- ', interval)
            interval_insights_dict = {}
            interval_max = {}
            interval_min = {}
            # print(instruments[interval].keys())
            interval_max['time'], interval_max['return']= max_return_calc(instruments[interval])
            interval_min['time'], interval_min['return']= min_return_calc(instruments[interval])
            interval_insights_dict['max_return'] = interval_max
            interval_insights_dict['min_return'] = interval_min
            time_heat_map_insights.update_one({"instrument_token":instruments["instrument_token"]},{"$set":{interval:interval_insights_dict}}) 
            # insights_dict[interval] = interval_insights_dict
            #print(insights_dict)
        # time_heat_map_insights.insert_one(insights_dict)
        
        #break

def max_return_calc(interval_dict):
    max_return = 0
    max_return_t = 0
    for keys in interval_dict.keys():
        if interval_dict[keys]['average_return'] > max_return:
            max_return_t = keys
            max_return = interval_dict[keys]['average_return'] 
    return max_return_t, max_return

def min_return_calc(interval_dict):
    min_return = 0
    min_return_t = 0
    for keys in interval_dict.keys():
        if interval_dict[keys]['average_return'] < min_return:
            min_return_t = keys
            min_return = interval_dict[keys]['average_return']
    return min_return_t, min_return

time_insights(time_heat_map)
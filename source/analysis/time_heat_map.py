#............ Calculates average return for every time interval for every stock and store in the DB

import pymongo
import datetime
import numpy as np

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
historical_col = myclient["core"]["historical_data"]
time_heat_map = myclient["core"]["analytics"]["time_heat"]
functional_data_col =myclient["core"]["functional"]
# time_heat_map.delete_many({})
#functional_data_col.delete_many({})

intervals = ['minute', 'day', '3minute', '5minute', '10minute', '15minute', '30minute', '60minute']


def create_time_heat_map(hist_coll):

    max_count_per_interval = {'minute': 0, 'day': 0, '3minute': 0, '5minute': 0, '10minute': 0, '15minute': 0, '30minute': 0, '60minute':0}
    
    for instruments in hist_coll.find({},{"_id":0}):
        heat_map_dict = {}
        heat_map_dict["tradingsymbol"] = instruments["tradingsymbol"]
        heat_map_dict["name"] = instruments["name"]
        heat_map_dict["instrument_token"] = instruments["instrument_token"]
        for interval in intervals:
            unique_intervals = {}
            for unit_intervals in instruments[interval]:
                #print(unit_intervals)
                ist_unit_intervals = convert_to_ist(unit_intervals['date'].time())
                open_price = unit_intervals['open']
                close_price = unit_intervals['close'] 
                interval_returns = calc_interval_returns(open_price,close_price)
                #print(interval_returns)
                if ist_unit_intervals not in unique_intervals:
                    unique_intervals[ist_unit_intervals] = [interval_returns]
                else:
                    unique_intervals[ist_unit_intervals].append(interval_returns)
              
            for intervals_keys in unique_intervals.keys():
                # print('Processing: instrument- ', instruments["tradingsymbol"], ' interval- ', interval, ' interval unit- ', intervals_keys)
                interval_keys_dict = {}
                interval_keys_dict['average_return'] = average_from_list(unique_intervals[intervals_keys])
                interval_keys_dict['count'] = np.size(unique_intervals[intervals_keys])
                if max_count_per_interval[interval] < np.size(unique_intervals[intervals_keys]):
                    max_count_per_interval[interval] = np.size(unique_intervals[intervals_keys])
                    print(max_count_per_interval,interval,np.size(unique_intervals[intervals_keys]))
                unique_intervals[intervals_keys] = interval_keys_dict

            
            # heat_map_dict[interval] = unique_intervals
            time_heat_map.update_one({"instrument_token":instruments["instrument_token"]},{"$set":{interval:unique_intervals}})               
        
        #print(heat_map_dict)
        # time_heat_map.insert_one(heat_map_dict)
    # functional_data = {}
    # functional_data['description'] = 'Max count per interval'
    # functional_data['variable'] = 'max_count_per_interval' 
    # functional_data['values'] = max_count_per_interval
    functional_data_col.update_one({"variable":"max_count_per_interval"},{"$set":{"values":max_count_per_interval}})

def calc_interval_returns(open_price, close_price):
    if open_price == 0:
        return 0
    else:
        return (close_price-open_price)/open_price

def convert_to_ist(gmt_time):
    ist_hour = 0
    ist_min = 0
    hour = gmt_time.hour
    min = gmt_time.minute
    if int((min+30)/60) == 0:
        ist_min = min+30
        if int((hour+5)/23) == 0: 
            ist_hour = hour+5
        else:
            ist_hour = (hour+5)%24
    else:
        ist_min = (min+30)%60
        if int((hour+6)/23) == 0: 
            ist_hour = hour+6
        else:
            ist_hour = (hour+6)%24
    #print(gmt_time, datetime.time(ist_hour,ist_min))
    return datetime.time(ist_hour,ist_min).strftime('%H:%M')

def average_from_list(returns_list):
    #print(np.sum(returns_list),np.size(returns_list))
    if np.size(returns_list) == 0:
        return 0.0
    else:
        return np.sum(returns_list)/np.size(returns_list)

create_time_heat_map(historical_col)




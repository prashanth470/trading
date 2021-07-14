import logging
import pymongo
from kiteconnect import KiteConnect
import datetime

logging.basicConfig(level=logging.DEBUG)

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
historical_col = myclient["core"]["historical_data"]
functional_data_col = myclient["core"]["functional"]

intervals = ['minute', 'day', '3minute', '5minute', '10minute', '15minute', '30minute', '60minute']

api_key = functional_data_col.find({"variable":"api_key"},{"_id":0, "values":1})[0]['values']
api_secret = functional_data_col.find({"variable":"api_secret"},{"_id":0, "values":1})[0]['values']
access_token = functional_data_col.find({"variable":"access_token"},{"_id":0, "values":1})[0]['values']

kite = KiteConnect(api_key=api_key)

kite.set_access_token(access_token)

def update_historical_data(write_coll):
    to_date = datetime.datetime.today().strftime('%Y-%m-%d')
    for instruments in write_coll.find({},{"_id":0}):
        from_date = instruments['to_date']
        write_coll.update_one({"instrument_token":instruments["instrument_token"]}, {"$set":{'to_date':to_date}})
        for interval in intervals:
            print("Processing: ", instruments["tradingsymbol"], " interval: ", interval, " from_date: ", from_date, " to_date: ", to_date)
            try:
                entries = get_historical_data(instruments["instrument_token"], from_date, to_date, interval)
                new_entries = instruments[interval]+entries  
                #print(new_entries)         
                write_coll.update_one({"instrument_token":instruments["instrument_token"]},{"$set":{interval:new_entries}})       
            except Exception as e:
                print(e)
                pass

def get_historical_data(instrument_token, from_date, to_date, interval):
    return kite.historical_data(instrument_token, from_date, to_date, interval)

def store_historical_data(write_dict, write_coll):
    write_coll.insert_one(write_dict)
            
            

update_historical_data(historical_col)


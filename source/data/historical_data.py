import logging
import pymongo
from kiteconnect import KiteConnect
import datetime

logging.basicConfig(level=logging.DEBUG)

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
indices_col = myclient["core"]["indices"]
historical_col = myclient["core"]["historical_data"]
functional_data_col = myclient["core"]["functional"]

time_difference = 60
intervals = ['minute', 'day', '3minute', '5minute', '10minute', '15minute', '30minute', '60minute']

api_key = functional_data_col.find({"variable":"api_key"},{"_id":0, "values":1})[0]['values']
api_secret = functional_data_col.find({"variable":"api_secret"},{"_id":0, "values":1})[0]['values']
access_token = functional_data_col.find({"variable":"access_token"},{"_id":0, "values":1})[0]['values']

kite = KiteConnect(api_key=api_key)

kite.set_access_token(access_token)
# historical_col.delete_many({})



def parse_historical_data(read_coll):
    from_date = datetime.datetime.strftime(datetime.datetime.now()-datetime.timedelta(time_difference), '%Y-%m-%d')
    to_date = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d')
    for instruments in read_coll.find({},{"_id":0, "instrument_token":1, "tradingsymbol":1, "name":1, "mis_status":1}):
        try:
            # print(instruments)
            mis_status = instruments["mis_status"]
            if mis_status == "true":
                instrument_dict = {}
                instrument_dict["tradingsymbol"] = instruments["tradingsymbol"]
                instrument_dict["name"] = instruments["name"]
                instrument_dict["instrument_token"] = instruments["instrument_token"]
                instrument_dict["from_date"] = from_date
                instrument_dict["to_date"] = to_date
                for interval in intervals:
                    print("Processing: ", instruments["name"], " interval: ", interval)
                    instrument_dict[interval] = {}
                    try:
                        instrument_dict[interval] = get_historical_data(instruments["instrument_token"], from_date, to_date, interval)           
                    except Exception as e:
                        instrument_dict[interval] = e
                        print(e)
                        pass
                store_historical_data(instrument_dict, historical_col)  
        except Exception as e:
            print(e)
            pass
                 
def get_historical_data(instrument_token, from_date, to_date, interval):
    return kite.historical_data(instrument_token, from_date, to_date, interval)

def store_historical_data(write_dict, write_coll):
    write_coll.insert_one(write_dict)
    

parse_historical_data(indices_col)

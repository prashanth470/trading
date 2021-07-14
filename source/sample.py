import logging
from kiteconnect import KiteConnect
import datetime
import pymongo

instrument_token = "738561" 
from_date = "2021-04-01"
to_date = "2021-06-30"
interval = '5minute'

logging.basicConfig(level=logging.DEBUG)

api_key = "kpgos7e4vbsaam5x"
api_secret = "t9092opsldr1huxk1bgopmitovurftto"
request_token = "qRQhzRYukvQetbXDhiRYJI4XgLhwX51k"
access_token = "gP5gr51tDMpYiPBKTH95oNluvzS20c6Y"

kite = KiteConnect(api_key=api_key)

# data = kite.generate_session(request_token, api_secret=api_secret)

# print(data)
kite.set_access_token(access_token)

print(kite.quote(['NSE:INFY']))

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
functional_col =myclient["core"]["functional"]
functional_data = {}
functional_data['description'] = 'Price limit for trading'
functional_data['variable'] = 'price_limit' 
functional_data['values'] = 20
functional_col.insert_one(functional_data)
#print(kite.historical_data(instrument_token, from_date, to_date, interval, continuous=False, oi=True))
# print(datetime.datetime.now().strftime('%H:%M'))
# print(datetime.datetime.strptime('13:19', '%H:%M').strftime(('%H:%M')))
# print(datetime.datetime.now().strftime('%H:%M') == datetime.datetime.strptime('13:19', '%H:%M').strftime(('%H:%M')))
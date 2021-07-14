import pymongo
import datetime
import uuid
import logging
from kiteconnect import KiteConnect


myclient = pymongo.MongoClient("mongodb://localhost:27017/")
stocks_for_interval = myclient["core"]["analytics"]["interval_stocks"]
daily_trades = myclient["core"]["trade"]["generation"]
functional_data_col = myclient["core"]["functional"]
return_values = ['max_return', 'min_return']
price_limit = functional_data_col.find({"variable":"price_limit"},{"_id":0, "values":1 })[0]['values']
quantity_limit = 1
daily_trades.delete_many({})

api_key = functional_data_col.find({"variable":"api_key"},{"_id":0, "values":1})[0]['values']
api_secret = functional_data_col.find({"variable":"api_secret"},{"_id":0, "values":1})[0]['values']
access_token = functional_data_col.find({"variable":"access_token"},{"_id":0, "values":1})[0]['values']

kite = KiteConnect(api_key=api_key)

kite.set_access_token(access_token)


def create_daily_trades(interval_col, trade_col):
    for interval in interval_col.find({},{"_id":0}):
        interval_value = interval['interval']
        if interval_value != 'day':
            for interval_units in interval_col.find({"interval":interval_value},{"_id":0,"interval":0}):
                for interval_unit in interval_units:
                    # print('Processing: interval: ', interval['interval'], ', interval unit: ', interval_unit)
                    interval_buy_order = {}
                    interval_sell_order = {}
                    interval_buy_order["time"] = interval_unit
                    interval_buy_order["interval"] = interval['interval']
                    interval_buy_order["trade_type"] = 'mother_trade'
                    interval_buy_order["trade_id"] = str(uuid.uuid4())
                    interval_sell_order["time"] = interval_unit
                    interval_sell_order["interval"] = interval['interval']
                    interval_sell_order["trade_type"] = 'mother_trade'
                    interval_sell_order["trade_id"] = str(uuid.uuid4())
                    for items in return_values:
                        if items == 'max_return':
                            # print(interval_units[interval_unit][items])
                            quantity_buy, trade_decider_buy = decide_quantity_and_price_limit(interval_units[interval_unit][items]["tradingsymbol"])
                            if trade_decider_buy:
                                interval_buy_order["order_type"] = 'BUY'
                                interval_buy_order["tradingsymbol"] = interval_units[interval_unit][items]["tradingsymbol"]
                                interval_buy_order["name"] = interval_units[interval_unit][items]["name"]
                                interval_buy_order["instrument_token"] = interval_units[interval_unit][items]["instrument_token"]
                                interval_buy_order["quantity"] = quantity_buy
                                interval_buy_order["expected_return"] = interval_units[interval_unit][items]["returns"]
                                interval_buy_order["trade_generated_time"] = datetime.datetime.now()
                                trade_col.insert_one(interval_buy_order)
                        if items == 'min_return':
                            # print(interval_units[interval_unit][items])
                            quantity_sell, trade_decider_sell = decide_quantity_and_price_limit(interval_units[interval_unit][items]["tradingsymbol"])
                            if trade_decider_sell:
                                interval_sell_order["order_type"] = 'SELL'
                                interval_sell_order["tradingsymbol"] = interval_units[interval_unit][items]["tradingsymbol"]
                                interval_sell_order["name"] = interval_units[interval_unit][items]["name"]
                                interval_sell_order["instrument_token"] = interval_units[interval_unit][items]["instrument_token"]
                                interval_sell_order["quantity"] = quantity_sell
                                interval_sell_order["expected_return"] = -interval_units[interval_unit][items]["returns"]
                                interval_sell_order["trade_generated_time"] = datetime.datetime.now()
                                trade_col.insert_one(interval_sell_order)
                   
                    
        
def decide_quantity_and_price_limit(trading_symbol):
    price_limit_decider = True
    trading_symbol = 'NSE:'+trading_symbol
    last_traded_price = kite.quote([trading_symbol])[trading_symbol]['last_price']
    quantity = int(1000/last_traded_price)
    if quantity==0 or quantity==1:
        quantity += 1
    if last_traded_price < price_limit:
        price_limit_decider = False
    # print(trading_symbol, last_traded_price, quantity)
    return quantity, price_limit_decider


# print(decide_quantity_and_price_limit('INFY'))
create_daily_trades(stocks_for_interval, daily_trades)
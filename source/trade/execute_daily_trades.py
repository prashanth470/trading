import pymongo
import datetime
import copy
import logging
import uuid
from kiteconnect import KiteConnect

logging.basicConfig(level=logging.DEBUG)

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
daily_trades = myclient["core"]["trade"]["generation"]
exec_trade = myclient["core"]["trade"]["executed"]
functional_data_col = myclient["core"]["functional"]

interval_mapping = {'minute':1, 'day':0, '3minute':3, '5minute':5, '10minute':10, '15minute':15, '30minute':30, '60minute':60}
# exec_trade.delete_many({})

api_key = functional_data_col.find({"variable":"api_key"},{"_id":0, "values":1})[0]['values']
api_secret = functional_data_col.find({"variable":"api_secret"},{"_id":0, "values":1})[0]['values']
access_token = functional_data_col.find({"variable":"access_token"},{"_id":0, "values":1})[0]['values']

kite = KiteConnect(api_key=api_key)

kite.set_access_token(access_token)


def execute_trades(trades_col, exc_col):
    while trades_col.find({},{"_id":0}):
        try:
            for current_trade in trades_col.find({},{"_id":0}):
            # current_trade =trades_col.find({},{"_id":0})[0]
                current_trade_time = current_trade['time']
                current_trade_interval = current_trade['interval']
                current_trade_order_type = current_trade['order_type']
                current_trade_trading_symbol = current_trade['tradingsymbol']
                current_trade_quantity = current_trade['quantity']
                current_trade_type = current_trade['trade_type']
                
                current_trade_id = current_trade['trade_id']
                if current_trade_interval!= 'day':
                    if datetime.datetime.now().strftime('%H:%M') == datetime.datetime.strptime(current_trade_time, '%H:%M').strftime(('%H:%M')):
                        print(current_trade_type)
                        try:
                            if current_trade_order_type == 'BUY':
                                order_id = kite.place_order(variety=kite.VARIETY_REGULAR, 
                                                            exchange=kite.EXCHANGE_NSE, 
                                                            order_type=kite.ORDER_TYPE_MARKET,
                                                            tradingsymbol=current_trade_trading_symbol, 
                                                            transaction_type=kite.TRANSACTION_TYPE_BUY,
                                                            quantity=current_trade_quantity,
                                                            validity=kite.VALIDITY_DAY,
                                                            product=kite.PRODUCT_MIS)
                            else:
                                order_id = kite.place_order(variety=kite.VARIETY_REGULAR, 
                                                            exchange=kite.EXCHANGE_NSE, 
                                                            order_type=kite.ORDER_TYPE_MARKET,
                                                            tradingsymbol=current_trade_trading_symbol, 
                                                            transaction_type=kite.TRANSACTION_TYPE_SELL,
                                                            quantity=current_trade_quantity,
                                                            validity=kite.VALIDITY_DAY,
                                                            product=kite.PRODUCT_MIS)
                            print("Processing trade id: ", current_trade_id)
                            executed_trade ={}
                            executed_trade['executed_time'] = datetime.datetime.now()
                            executed_trade['trade_time'] = current_trade_time
                            executed_trade['trade_interval'] = current_trade_interval
                            executed_trade['trade_type'] = current_trade_type
                            executed_trade['trade_order_type'] = current_trade_order_type
                            executed_trade['tradingsymbol'] = current_trade_trading_symbol
                            executed_trade['trade_quantity'] = current_trade_quantity
                            executed_trade['trade_id'] = current_trade_id
                            exc_col.insert_one(executed_trade)
                            if  current_trade_type == 'mother_trade':
                                exit_trade = generate_exit_trade(current_trade)
                                # print(current_trade)
                                # print(exit_trade)
                                trades_col.insert_one(exit_trade)
                            trades_col.delete_one({"trade_id":current_trade_id})
                        except Exception as e:
                            pass
                #---- Execute trade here
                        
                    else:
                        pass
                else:
                    trades_col.delete_one({"trade_id":current_trade_id})
        except Exception as e:
            print(e)
            

        # break

def generate_exit_trade(trade_order):
    exit_trade = copy.deepcopy(trade_order)
    trade_time = datetime.datetime.strptime(trade_order['time'], '%H:%M')
    trade_interval = trade_order['interval']
    trade_order_type = trade_order['order_type']
    if trade_order_type == 'BUY':
        exit_trade['order_type'] = 'SELL'
    else:
        exit_trade['order_type'] = 'BUY'
    exit_trade['time'] = change_time(trade_time,trade_interval)
    exit_trade['trade_type'] = 'exit_trade'
    exit_trade['trade_generated_time'] = datetime.datetime.now()
    exit_trade['trade_id'] = str(uuid.uuid4())
    return exit_trade
    
def change_time(time, interval):
    out_min = 0
    out_hour = 0
    hour = time.hour
    min = time.minute
    if int((min+interval_mapping[interval])/60) == 0:
        out_min = min+interval_mapping[interval]
        out_hour = hour
    else:
        out_min = (min+interval_mapping[interval])%60
        out_hour = hour+1
    #print(gmt_time, datetime.time(ist_hour,ist_min))
    return datetime.time(out_hour,out_min).strftime('%H:%M')

execute_trades(daily_trades, exec_trade)
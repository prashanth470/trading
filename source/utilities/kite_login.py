import pymongo
from kiteconnect import KiteConnect
import logging

logging.basicConfig(level=logging.DEBUG)

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
functional_db = myclient["core"]["functional"]

api_key = functional_db.find({"variable":"api_key"},{"_id":0, "values":1})[0]['values']
api_secret = functional_db.find({"variable":"api_secret"},{"_id":0, "values":1})[0]['values']
request_token = "CdlX6aS4Mf9x3Vy80y0eNR29k1XJiKYe"


api_url = "https://kite.trade/connect/login?api_key=kpgos7e4vbsaam5x"

kite = KiteConnect(api_key=api_key)

def update_login_credentials(rqst_token):
    login_data = kite.generate_session(rqst_token, api_secret=api_secret)
    access_token = login_data["access_token"]
    print(access_token)
    functional_db.update_one({"variable":"request_token"},{"$set":{"values":rqst_token}})
    functional_db.update_one({"variable":"access_token"},{"$set":{"values":access_token}})

update_login_credentials(request_token)




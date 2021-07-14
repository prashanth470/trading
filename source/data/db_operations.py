import pymongo
from data_handlers import import_from_csv

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
core_db = myclient["core"]
indices_col = core_db["indices"]
historical_data_col = core_db["historical_data"]

instruments_file = "C:\\Users\\Prathiksha\\Documents\\Prashanth\\Trading\\instruments_nse.csv"
market_cap_file = "C:\\Users\\Prathiksha\\Documents\\Prashanth\\Trading\\MCAP31032021_0.csv"
mis_data_file = "C:\\Users\\Prathiksha\\Documents\\Prashanth\\Trading\\Zerodha - Intraday margins - EQ- MIS_CO leverages.csv"
instruments_df = import_from_csv(instruments_file)
market_cap_df = import_from_csv(market_cap_file)
mis_df = import_from_csv(mis_data_file)
print(mis_df.columns)

def insert_to_db(df, db_col):
    indices = df.columns
    for ind in df.index:
        input_dict = {}
        for index in indices:
            input_dict[index] = str(df[index][ind])
        db_col.insert_one(input_dict)

def update_to_db(df, db_col, indices, field):
    for ind in df.index:
        for i in range(0, len(indices)):
            try:
                db_col.update_one({"tradingsymbol":df[indices[i]][ind]},{"$set":{field:"true"}})
            except Exception as e:
                print(e)
            # try:
            #     db_col.update_one({"tradingsymbol":df[indices[i]][ind]+"-BE"},{"$set":{"rank":df['Sr. No.'][ind], "market_cap":df['market_cap'][ind]}})
            # except Exception as e:
            #     print(e)
                
    
update_to_db(mis_df, indices_col, ['Symbol'], "mis_status")
#insert_to_db(instruments_df, indices_col)



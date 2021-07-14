print("Retrieving new data.....")
# from data import update_historical_data
print("Retrieved new data")
print("Retrieving time interval data.....")
# from analysis import time_heat_map
print("Retrieved time interval data")
print("Creating time heat map.....")
# from analysis import time_heat_map_insights
print("Time heats generated and stored in DB")
print("Analysing trades for time interval.....")
# from analysis import interval_stocks
print("Trades analyzed for each interval and stored in DB")
print("Generating new trades.....")
from trade import generate_daily_trades
print("Trades generated and stored in DB")
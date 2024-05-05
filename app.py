import threading
import time
from serverFns import *

from flask import Flask, request, jsonify


STOCK_DATA_COLLECTION_NAME = "StockData"
APP_REQ_DATA_DIR = "AppReqData"
running = True

# Updater Thread Functions
def stock_data_updater_thread() : 
    global running
    while running : 
        updateAllFirebaseStockData(STOCK_DATA_COLLECTION_NAME)
        time.sleep(60*60*24) # Sleep for 24 hours

def stock_predict_updater_thread() : 
    global running
    while running : 
        updateAllFirebaseStockPredictions(STOCK_DATA_COLLECTION_NAME)
        # Weekly Update
        time.sleep(60*60*24*7)

def update_trending_stocks_thread() : 
    global running
    while running : 
        print("Updating Trending Stocks!")
        trendingStocks = getTopStocks(STOCK_DATA_COLLECTION_NAME, 7, 10)
        trendingStocksDict = {
            "trendingStocks" : trendingStocks,
        }
        try : 
            with open(APP_REQ_DATA_COLLECTION_NAME+"/trendingStocks.json", "w") as f:
                json.dump(trendingStocksDict, f)
                f.close()
                
            print("Trending Stocks Updated!")
        except Exception as e : 
            print("Couldnt Update Trending Stocks!")
            print(e)
            
        time.sleep(60*60*24) # Sleep for 24 hours

def update_top_stocks_thread() : 
    global running
    while running : 
        print("Updating Top Stocks!")
        topStocks = getTopStocks(STOCK_DATA_COLLECTION_NAME, 30, 10)
        topStocksDict = {
            "topStocks" : topStocks,
        }
        try : 
            with open(APP_REQ_DATA_COLLECTION_NAME+"/topStocks.json", "w") as f:
                json.dump(topStocksDict, f)
                f.close()
            
            print("Top Stocks Updated!")
        except Exception as e : 
            print("Couldnt Update Top Stocks!")
            print(e)
            
        time.sleep(60*60*24) # Sleep for 24 hours


def stop_threads() : 
    global running
    running = False


# Flask App
app = Flask(__name__)

@app.route("/", methods = ["GET"])
def index() : 
    return "Hello World!"
    
@app.route("/api/getTopStocks", methods = ["GET"])
def get_top_stocks() : 
    days = int(request.args.get("days", default=30))
    n = int(request.args.get("nTopStocks", default=10))
    topStocks = getTopStocks(STOCK_DATA_COLLECTION_NAME, days, n)
    return jsonify(topStocks)
    
@app.route("/api/getFutureTopStocks", methods = ["GET"])
def get_future_top_stocks() : 
    months = int(request.args.get("months"))
    n = int(request.args.get("nTopStocks"))
    topStocks = getFutureTopStocks(STOCK_DATA_COLLECTION_NAME, months, n)
    return jsonify(topStocks)

@app.route("/api/getStockData", methods = ["GET"])
def get_stock_data() : 
    ticker = request.args.get("ticker")
    stockData = getStockData(ticker, STOCK_DATA_COLLECTION_NAME)
    return jsonify(stockData)

@app.route("/api/getStockPortfolioData", methods = ["GET"])
def get_stock_portfolio_data() : 
    ticker = request.args.get("ticker")
    stockPortfolioData = getStockPortfolioData(ticker, STOCK_DATA_COLLECTION_NAME)
    return jsonify(stockPortfolioData)

@app.route("/api/fetchTrendingStocks", methods = ["GET"])
def fetch_trending_stocks() : 
    with open(APP_REQ_DATA_DIR+"/trendingStocks.json") as f: 
        trendingStocks = json.load(f)
        f.close()
        
    return jsonify(trendingStocks)
    
@app.route("/api/fetchTopStocks", methods = ["GET"])
def fetch_top_stocks() :
    with open(APP_REQ_DATA_DIR+"/topStocks.json") as f: 
        topStocks = json.load(f)
        f.close()
        
    return jsonify(topStocks)
    
if __name__ == "__main__":
    
    # # Starting the Stock Data Updater Thread
    # stockDataUpdaterThread = threading.Thread(target=stock_data_updater_thread)
    # stockDataUpdaterThread.start()
    
    # # Starting the Stock Predcition Updater Thread
    # time.sleep(10)
    # stockPredictUpdaterThread = threading.Thread(target=stock_predict_updater_thread)
    # stockPredictUpdaterThread.start()
    
    # # Starting the Trending Stocks Updater Thread
    # time.sleep(10)
    trendingStocksUpdaterThread = threading.Thread(target=update_trending_stocks_thread)
    trendingStocksUpdaterThread.start()
    
    # # Starting the Top Stocks Updater Thread
    time.sleep(10)
    topStocksUpdaterThread = threading.Thread(target=update_top_stocks_thread)
    topStocksUpdaterThread.start()
    
    # Running the Flask App
    app.run(host="0.0.0.0", port = 5000, debug=True)
    
import pandas
from datetime import datetime, date, timedelta
import yfinance
import json
import time
from prophet import Prophet
from dateutil.relativedelta import relativedelta
import os
import threading

from firebase_admin import firestore, credentials, initialize_app


# Initialize Firestore with your project credentials
# Loading SDK from env
firebaseSDKJson = json.loads(os.getenv("firebaseSDK") )
cred = credentials.Certificate(firebaseSDKJson)
initialize_app(cred)

DATA_UPDATE_LOG_FILE_PATH = "./logs/dataUpdateLog.txt"
PREDICTION_UPDATE_LOG_FILE_PATH = "./logs/predictionUpdateLog.txt"

# Common Functions
getStockCurrPrice = lambda stockJson : stockJson["historicalData"][-1]["Close"]

def updateStockDataDict(jsonDataDict: dict, new_date: date = datetime.now().date() ) -> dict : 
    """
    Update the stock data by fetching new data from an API and appending it to the existing data.

    Parameters:
    dataJsonPath (str): The file path of the JSON file containing the stock data.

    Returns:
    dict: The updated stock data dictionary.

    """
    
    if (type(jsonDataDict) == str) : 
        stockData = json.loads(jsonDataDict)
    elif (type(jsonDataDict) == dict ) : 
        stockData = jsonDataDict
        
    if (type(new_date) == str) : 
        new_date = datetime.strptime(new_date, "%Y-%m-%d").date()
        
    # Getting the last update date String
    lastUpdateDate = stockData["lastDataUpdateDate"]
    # Converting to datetime object
    lastUpdateDate = datetime.strptime(lastUpdateDate, "%Y-%m-%d")
    startDate = lastUpdateDate + timedelta(days=1)
        
    ticker = stockData["ticker"]
    
    # If the last update date is not today, then update the data
    if lastUpdateDate.date() != new_date :
        print("Fetching Data form the YFinance...")
        # Getting the data from the API
        yfTicker = yfinance.Ticker(ticker)
        data:pandas.DataFrame = yfTicker.history(start=startDate, end=new_date, interval="1d")
        
        # Only make Changes if the data is not empty
        if not(data.empty) :
            # Converting the Data to a List of Dictionaries
            dataRecordList = data.to_dict(orient="records")
            date_vals = data.index
            # Including the dates as params in each obj
            for i in range(len(dataRecordList) ) : 
                dataRecordList[i]["Date"] = date_vals[i].strftime("%Y-%m-%d")
            
            # Updating the last update date
            stockData["lastDataUpdateDate"] = new_date.strftime("%Y-%m-%d")
            # Joining with the existing Historical Data Dict
            stockData["historicalData"].extend(dataRecordList)
            
            print(f"Data Updated for {ticker} till {new_date}!")
            
        else : 
            print(f"No updates for {ticker}")
                        
    return stockData

def convert_stock_dict_to_FBDf (stockDict : dict) -> pandas.DataFrame : 
    histData = stockDict["historicalData"]
    
    complete_df = pandas.DataFrame(histData)
    FBP_train_df = complete_df[ ["Date","Close"] ]
    FBP_train_df = FBP_train_df.rename(columns={"Date" : "ds", "Close" : "y"})
    FBP_train_df["ds"] = pandas.to_datetime(FBP_train_df["ds"], format="%Y-%m-%d")
    
    return FBP_train_df

# def plot_data(pastData : pandas.DataFrame, futPredictedData : pandas.DataFrame, actualFutData = None) : 
#     # Plot the past Data as Blue Line
#     plt.plot(pastData["ds"], pastData["y"], label = "Past Data", color = "blue")
#     # Plot the Future Predicted Data as Red Line
#     plt.plot(futPredictedData["ds"], futPredictedData["y"], label = "Future Predicted Data", color = "red")
#     # Plot the Actual Future Data as Yellow Line
#     if (type(actualFutData) == pandas.DataFrame) : 
#         plt.plot(actualFutData["ds"], actualFutData["y"], label = "Actual Future Data", color = "yellow")

def FBProphet_predict(trainData : pandas.DataFrame, months: int = 12, fromDate:datetime = datetime.now() ) -> pandas.DataFrame:
    """
    Predicts future values using Facebook Prophet model.

    Parameters:
        trainingData (panadas.DataFrame) : the df to fit the FBProphet model. 
        months (int, optional): Number of months to predict into the future. Defaults to 12.
        fromDate (str | datetime, optional): Starting date for prediction. Defaults to current date and time.
        pastFutRatio (int, optional): Ratio of past data to consider for training the model. Defaults to 3.
        plotPredictions (bool, optional): Whether to plot the predicted values. Defaults to False.

    Returns:
        pandas.DataFrame: DataFrame containing the predicted values.

    Raises:
        FileNotFoundError: If the training data file for the given ticker symbol is not found.

    """

    # Handling Type
    if (type(fromDate) == str) : 
        curr_date : date = datetime.strptime(fromDate,"%d-%m-%Y")
    elif (type(fromDate) == datetime) : 
        curr_date = fromDate
    
        
    # Getting Relevant Tarining data
    refData = trainData[(trainData["ds"] < curr_date) ]
    
    # Creating the Model
    model = Prophet()
    model.fit(refData)
    
    # Creating the future DataFrame
    fut_days = int(365*(months/12))
    fut_df = model.make_future_dataframe(periods= fut_days)
    # Making the Prediction
    pred = model.predict(fut_df)
    
    # Selecting only the ds and the yhat columns
    pred = pred[ ["ds","yhat"] ]
    pred = pred.rename(columns={"yhat" : "y"})
    
    
    # if (plotPredictions) : 
    #     actualFutDF = trainData[(trainData["ds"] > fromDate) & (trainData["ds"] < (curr_date + relativedelta(days=fut_days) ) )]
    #     plot_data(pred[(pred["ds"] < fromDate)], pred[(pred["ds"] >= fromDate)], actualFutDF)
        
    return pred
    
def calculate_growth_from_FBPrediction (data : pandas.DataFrame, curr_date : datetime = datetime.now()) -> float : 
    """
    Calculate the percentage increase in the stock price from the given date to the current date.

    Parameters:
    data (pandas.DataFrame): DataFrame containing the stock data.
    curr_date (datetime, optional): Current date. Defaults to current date and time.

    Returns:
    float: Percentage increase in the stock price.

    """
    
    # Getting the last price
    pred_price = data["y"].iloc[-1]
    
    # Getting the current price
    curr_price = data[ (data["ds"] <= curr_date) ].iloc[-1]["y"]
    
    # Calculating the percentage increase
    percent_increase = ((pred_price - curr_price) / curr_price) * 100
    
    return percent_increase

def update_prediction_dict(stockData : dict) -> dict :
    FBP_train_data = convert_stock_dict_to_FBDf(stockData)
        
    # Updating Predictions for 3 months
    months = 3
    predData = FBProphet_predict(FBP_train_data, months)
    pred_value = predData["y"].iloc[-1]
    pred_value = round(pred_value, 2)
    percentIncrease = calculate_growth_from_FBPrediction(predData)
    percentIncrease = round(percentIncrease, 2)
    stockData["predictions"]["3months"] = {
        "value" : pred_value,
        "percentIncrease" : percentIncrease
    }
            
    # Updating Predictions for 6 months
    months = 6
    predData = FBProphet_predict(FBP_train_data, months)
    pred_value = predData["y"].iloc[-1]
    pred_value = round(pred_value, 2)
    percentIncrease = calculate_growth_from_FBPrediction(predData)
    percentIncrease = round(percentIncrease, 2)
    stockData["predictions"]["6months"] = {
        "value" : pred_value,
        "percentIncrease" : percentIncrease
    }
            
    # Updating Predictions for 1 Year
    months = 12
    predData = FBProphet_predict(FBP_train_data, months)
    pred_value = predData["y"].iloc[-1]
    pred_value = round(pred_value, 2)
    percentIncrease = calculate_growth_from_FBPrediction(predData)
    percentIncrease = round(percentIncrease, 2)
    stockData["predictions"]["1year"] = {
        "value" : pred_value,
        "percentIncrease" : percentIncrease
    }
            
    # updating Predictions for 2 years
    months = 24
    predData = FBProphet_predict(FBP_train_data, months)
    pred_value = predData["y"].iloc[-1]
    pred_value = round(pred_value, 2)
    percentIncrease = calculate_growth_from_FBPrediction(predData)
    percentIncrease = round(percentIncrease, 2)
    stockData["predictions"]["2years"] = {
        "value" : pred_value,
        "percentIncrease" : percentIncrease
    }
            
    # Updating Predictions for 3 years
    months = 36
    predData = FBProphet_predict(FBP_train_data, months)
    pred_value = predData["y"].iloc[-1]
    pred_value = round(pred_value, 2)
    percentIncrease = calculate_growth_from_FBPrediction(predData)
    percentIncrease = round(percentIncrease, 2)
    stockData["predictions"]["3years"] = {
        "value" : pred_value,
        "percentIncrease" : percentIncrease
    }
            
    # Updating Predictions for 5 years
    months = 60
    predData = FBProphet_predict(FBP_train_data, months)
    pred_value = predData["y"].iloc[-1]
    pred_value = round(pred_value, 2)
    percentIncrease = calculate_growth_from_FBPrediction(predData)
    percentIncrease = round(percentIncrease, 2)
    stockData["predictions"]["5years"] = {
        "value" : pred_value,
        "percentIncrease" : percentIncrease
    }
    
    
    # Updating the last Prediction Date
    stockData["lastPredictionsUpdateDate"] = datetime.now().strftime("%Y-%m-%d")
    
    return stockData

def new_update_prediction_dict(stockData : dict) -> dict :
    FBP_train_data = convert_stock_dict_to_FBDf(stockData)
    # Creating a Lock for the new Dictionary
    dictLock = threading.Lock()
    newPreds = {
        "3months" : {"value" : 0,"percentIncrease" : 0},
        "6months" : {"value" : 0,"percentIncrease" : 0},
        "1year" : {"value" : 0,"percentIncrease" : 0},
        "2years" : {"value" : 0,"percentIncrease" : 0},
        "3years" : {"value" : 0,"percentIncrease" : 0},
        "5years" : {"value" : 0,"percentIncrease" : 0}
    }
    
    def updatePredValues(months : int, trainData: pandas.DataFrame) :
        nonlocal newPreds
        # Getting Predictions
        predData = FBProphet_predict(trainData,months=months)
        pred_value = predData["y"].iloc[-1]
        pred_value = round(pred_value, 2)
        percentIncrease = calculate_growth_from_FBPrediction(predData)
        percentIncrease = round(percentIncrease, 2) 
        
        # GeneratingKeyStr
        keyStr = ""
        if months < 12 :
            keyStr = f"{months}months"
        elif months > 12 :
            keyStr = f"{months//12}years"
        else :
            keyStr = "1year"
        
        dictLock.acquire()
        
        try : 
            newPreds[keyStr] = {
                "value" : pred_value,
                "percentIncrease" : percentIncrease
            }
        finally :
            dictLock.release()
            print(f"Prediction for {keyStr} Updated!")
        
    # Create Threads for each Prediction
    threads = []
    months = [3,6,12,24,36,60]
    for month in months :
        thread = threading.Thread(target=updatePredValues, args=(month, FBP_train_data))
        threads.append(thread)
        thread.start()
        time.sleep(0.05)
        
    # Joining the Threads
    for thread in threads :
        thread.join() 
    
    # Updating the predictionValues in actual Dict
    stockData["predictions"] = newPreds
            
    # Updating the last Prediction Date
    stockData["lastPredictionsUpdateDate"] = datetime.now().strftime("%Y-%m-%d")
    
    return stockData

def filterHistoricalData(historicalData : list, fromDate : datetime, toDate : datetime = datetime.now().date() ) -> list : 
    # Filtering the Historical data between the from and to dates
    filteredData = [data for data in historicalData if fromDate <= datetime.strptime(data['Date'], "%Y-%m-%d") <= toDate]
    return filteredData

def calculateStockGrowth(stockDict : dict, fromDate : datetime, toDate : datetime = datetime.now().date() ) -> float: 
    historicalData = stockDict["historicalData"]
    # Filtering the Historical data between the from and to dates
    filteredData = filterHistoricalData(historicalData, fromDate, toDate)
    
    # Calculating the percent increase
    percentGrowth = ((filteredData[-1]["Close"] - filteredData[0]["Close"]) / filteredData[0]["Close"]) * 100
    
    return round(percentGrowth, 2)

def alterDataForCurrTopStocks (stockData : dict, days : int = 7) -> dict : 
    curr_price = stockData["historicalData"][-1]["Close"]
    # Calculating the percent increase
    to_date = datetime.strptime(stockData["historicalData"][-1]["Date"],"%Y-%m-%d")
    from_date = to_date - timedelta(days = days)
    date1yrPast = to_date - relativedelta(years=1)
    # Filtering the data based on the date
    # filteredData = list(filter(lambda obj : datetime.strptime(obj["Date"],"%Y-%m-%d") >= date1yrPast, stockData["historicalData"]))
    percentGrowth = calculateStockGrowth(stockData, from_date, to_date)
        
    # Adding currPrice and percentGrowth to the stockData
    stockData["currPrice"] = curr_price
    stockData["percentGrowth"] = percentGrowth
    # Updating the historical Data
    # stockData["historicalData"] = filteredData
    
    return stockData

# Log Functions
def logData(s : str, logFileName : str) :
    with open(logFileName, "a") as f : 
        f.write(s + "\n")
        f.close()

def clearLog(logFileName : str) :
    with open(logFileName, "w") as f : 
        f.write("")
        f.close()
        

# Updater Functions
def updateAllFirebaseStockData(stockDataCollectionName:str,tillDate:date = datetime.now().date() ) : 
    """
    Update all the stock data in the Firestore database.

    """
    clearLog(DATA_UPDATE_LOG_FILE_PATH)
    logData(f"Firebase Data Update Log for {tillDate} ", DATA_UPDATE_LOG_FILE_PATH)
    
    # Getting the Firestore Database
    db = firestore.client()
    # Getting the Collection Reference
    stockDataCollection = db.collection(stockDataCollectionName)
    # Getting the List of all tickers
    tickersList = stockDataCollection.document("tickersList").get().to_dict()["tickers"]
    
    logData("Tickers Fetched from Firestore!", DATA_UPDATE_LOG_FILE_PATH)
    
    # Itterating over all the tickers
    for ticker in tickersList :
        # Getting the Document Reference
        docRef = stockDataCollection.document(ticker)
        # Getting the Document Data
        docData = docRef.get().to_dict()
        # Updating the Document Data
        updatedData = updateStockDataDict(docData, tillDate)
        # Updating the Document
        docRef.set(updatedData)
        logData(f"Data Updated for {ticker}!", DATA_UPDATE_LOG_FILE_PATH)
        
    logData("Data Updated Successfully!", DATA_UPDATE_LOG_FILE_PATH)    
    
def updateAllFirebaseStockPredictions (collectionName : str) : 
    clearLog(PREDICTION_UPDATE_LOG_FILE_PATH)
    logData(f"Firebase Prediction Update Log for {datetime.now().date()}", PREDICTION_UPDATE_LOG_FILE_PATH)
    
    db = firestore.client()
    # Getting the Collection Reference
    stockDataCollection = db.collection(collectionName)
    # Getting the List of Tickers
    tickersList = stockDataCollection.document("tickersList").get().to_dict()["tickers"]
    
    logData("Tickers Fetched from Firestore!", PREDICTION_UPDATE_LOG_FILE_PATH)
    
    for ticker in tickersList :
        logData(f"Updating Prediction for : {ticker}", PREDICTION_UPDATE_LOG_FILE_PATH)
        try : 
            # Getting the Document Reference
            stockData = stockDataCollection.document(ticker).get().to_dict()
            
            if (type(stockData) != None) : 
                # Getting Last UpdateDate
                last_pred_date = stockData["lastPredictionsUpdateDate"]
                # Converting to Datetime
                last_pred_date = datetime.strptime(last_pred_date, "%Y-%m-%d")
                
                # Updating if update date is not atleast a week old
                if (datetime.now() - last_pred_date).days >= 7 : 
                    updated_dict = new_update_prediction_dict(stockData)
                
                    # Updating the Document
                    stockDataCollection.document(ticker).set(updated_dict)
                    logData(f"Prediction Updated for {ticker}!", PREDICTION_UPDATE_LOG_FILE_PATH)
                    
                else : 
                    logData(f"No Update for {ticker}!", PREDICTION_UPDATE_LOG_FILE_PATH)
        except Exception as e : 
            logData(f"Error Updating Prediction for {ticker}!", PREDICTION_UPDATE_LOG_FILE_PATH)
            logData(e, PREDICTION_UPDATE_LOG_FILE_PATH)

def getTopStocks(collectionName : str, days : int = 7, n : int = 10) : 
    # Getting the Firestore Database
    db = firestore.client()
    try : 
        # Getting the Collection Reference
        stockDataCollection = db.collection(collectionName)
    except : 
        print(f"{collectionName} : No Collection Found!")
        return []
    
    # Getting the List of Tickers
    tickersList = stockDataCollection.document("tickersList").get().to_dict()["tickers"]
    
    stockDataList = []
    
    for ticker in tickersList :
        # Getting the Document Data
        docData = stockDataCollection.document(ticker).get().to_dict()
        docData = alterDataForCurrTopStocks(docData, days)
        stockDataList.append(docData)
        
    # Sorting the Stock Data List Based on Percentage Growth
    stockDataList.sort(key = lambda x : x["percentGrowth"], reverse = True)
    
    return stockDataList[:n]
    
def getFutureTopStocks(stockDataCollectionName:str, months : int = 12,topN:int = 10) : 
    """
    Get the top N stocks based on the stock data in the Firestore database.

    """
    # Getting the Firestore Database
    db = firestore.client()
    try : 
        # Getting the Collection Reference
        stockDataCollection = db.collection(stockDataCollectionName)
    except : 
        print(f"{stockDataCollectionName} : No Collection Found!")
        return []
    
    # Getting the List of Tickers
    tickersList = stockDataCollection.document("tickersList").get().to_dict()["tickers"]
        
    stockDataList = []
    
    for ticker in tickersList :
        # Getting the Document Data
        docData = stockDataCollection.document(ticker).get().to_dict()
        stockDataList.append(docData)
            
    # Sorting the Stock Data List Based of Predeicted Values
    keyVal = ""
    if months < 12 : 
        keyVal = f"{months}months"
    elif months > 12 : 
        keyVal = f"{months//12}years"
    else : 
        keyVal = "1year"
        
    stockDataList.sort(key = lambda x : x["predictions"][keyVal]["percentIncrease"], reverse = True)
    
    return stockDataList[:topN]

# API Functions
def getStockPortfolioData (ticker : str, collectionName : str) -> dict : 
    filename = ticker.replace(".","_")
    db = firestore.client()
    stockDataCollection = db.collection(collectionName)
    
    try : 
        stockData = stockDataCollection.document(filename).get().to_dict()
        return {
            "stockName" : stockData["stockName"],
            "currPrice" : stockData["historicalData"][-1]["Close"],
            "iconURL" : stockData["iconURL"]
        }
    except FileNotFoundError :
        return {
            "error" : "Stock Data not found!"
        }

def recommendStocks (investmentAmt : int, months : int, nStocks : int ,collectionName : str) : 
    # Get all the stocks with currValue < investableAmount/5 
    # Basically Abiliy to purchase more quantities of the stock
    investableStockList = []
    
    # Fetching all Data in FireStore
    db = firestore.client()
    stockDataCollection = db.collection(collectionName)
    
    tickerList = stockDataCollection.document("tickersList").get().to_dict()["tickers"]
    
    for ticker in tickerList :
        # Getting the Document Reference
        stockData = stockDataCollection.document(ticker).get().to_dict()
                   
        if getStockCurrPrice(stockData) < (investmentAmt/5) : 
            # Adding Current Price to the stockData
            stockData["currPrice"] = getStockCurrPrice(stockData)
            # Calcuating the Percentage Growth over n months
            percentGrowth = calculateStockGrowth(stockData, datetime.now() - relativedelta(months=months), datetime.now() )
            stockData["percentGrowth"] = percentGrowth
            
            investableStockList.append(stockData)
    
    # Sort the stocks based on the predicted Future Growth in n months
    # Creating the key string
    keyStr = ""
    if (months > 12) :     
        keyStr = f"{months//12}years"
    elif (months < 12) : 
        keyStr = f"{months}months"
    else : 
        keyStr = "1year"
        
    # Sorting the data
    investableStockList.sort(key = lambda obj : obj["predictions"][keyStr]["percentIncrease"])
    investableStockList.reverse()
    
    return investableStockList[:nStocks]

def getStockData(ticker : str, collectionName : str) -> dict : 
    fileName = ticker.replace(".", "_")
    db = firestore.client()
    stockDataCollection = db.collection(collectionName)
    try : 
        stockData = stockDataCollection.document(fileName).get().to_dict()
        return stockData
    except Exception as e : 
        return {
            "error" : "Stock Data not found!"
        }  


if __name__ == "__main__" : 
    
    pass
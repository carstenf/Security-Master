import time
import math
import pandas as pd
from   sqlalchemy import create_engine
from tqdm import tqdm
import yfinance as yf
import zipfile
import timeit
import numpy as np
import os


# create the engine for the database
engine = create_engine('mysql+mysqlconnector://root:root@localhost/securities_master')

  
# if update == TRUE, find last date in trade_date collum and download from there until today
# ELSE import all avaiable data
update_DB = False

if update_DB == True:
    query = """SELECT MAX(trade_date) AS 'last_date' FROM securities_master.daily_price;"""
    update = pd.read_sql_query(query, engine)
    last_update = update.last_date[0]
    # just to bypass the last date in the collum and starts reading from where i want
    #last_update = '2020-02-04'


def get_symbol_security_id_quandl(qtable,engine):
## get ticker symbol and security_id

    # first decide which data vender to use and get the data_vendor_id
    # query data_vendor_id from the data_vendor table
    query = """SELECT id FROM securities_master.data_vendor WHERE name = 'Yahoo';"""
    value = pd.read_sql_query(query, engine)
    data_vendor_id = value.id[0].astype(int)

    # query ticker symbols and the ticker_id from the security table
    query_1 = """SELECT ticker, id FROM securities_master.security WHERE """

    # choose the Quandel table, ticker for SF1=fundamental , SEP=price or SFP=ETF,INDEX
    query_2 = """ ttable = '{}' """.format( qtable )

    query_3 = """ and data_vendor_id = {} """.format( data_vendor_id )
    
    query = query_1 + query_2 + query_3

    # query securities_master
    result = pd.read_sql_query(query, engine)
    return result


def get_name_exchange_id():
    
    query = """SELECT id, name FROM securities_master.exchange;"""
    result = pd.read_sql_query(query, engine)
    
    return result
    

############################## fill the 3 small table Data Vendor, Asset Class and Exchange Table ########
## First step, define and populate 
# 1) data_vendor table
# 2) asset_class table
# 3) exchange table

# Define Data Vendors and populate data_vendor table

df_vendor=pd.DataFrame({'names': ['Quandl','Yahoo'], 'website': ['www.qandl.com', 'www.yahoo.com']})
# Fill Data Vendor
# initial
insert_init = """insert into data_vendor (name, website_url) values """
# Add values for all days to the insert statement
vals = ",".join(["""('{}','{}')""".format(row.names,row.website)for items,row in df_vendor.iterrows()]) 
# Handle duplicates - Avoiding errors if you've already got some data
# in your table
insert_end  = """  on duplicate key update name=values(name),website_url=values(website_url);"""
# Put the parts together
query = insert_init + vals + insert_end
# Fire insert statement
engine.execute(query)
Process=True


# Define Asset Classes and populate asset_class table
list_asset=['stocks','future']
# Fill Asset Class
# initial
insert_init = """insert into asset_class (asset_class) values """
# Add values for all days to the insert statement
vals = ",".join(["""('{}')""".format(items)for items in list_asset])     
# Handle duplicates - Avoiding errors if you've already got some data
# in your table
insert_end  = """  on duplicate key update asset_class=values(asset_class);"""
# Put the parts together
query = insert_init + vals + insert_end
# Fire insert statement
engine.execute(query)
Process=True

# Define Exchanges and populate exchange table
df_exchange=pd.DataFrame({'exchange': ['NYSE','NASDAQ','NYSEMKT','OTC','NYSEARCA','BATS','INDEX','None'], 'currency': ['USD','USD','USD','USD','USD','USD','P','None']})
# Fill Exchange
# initial
insert_init = """insert into exchange (name, currency) values """
# Add values for all days to the insert statement
vals = ",".join(["""('{}','{}')""".format(row.exchange,row.currency)for items,row in df_exchange.iterrows()]) 
# Handle duplicates - Avoiding errors if you've already got some data
# in your table
insert_end  = """  on duplicate key update name=values(name),currency=values(currency);"""
# Put the parts together
query = insert_init + vals + insert_end
# Fire insert statement
engine.execute(query)
Process=True

print('data_vendor, asset_class, exchange table filled')



############################################################## Fill Security / Ticker table 
## next step, need always updates if new ipo's were held
# Polulate security table with ticker symbols

# defintion of the asset class
# need to be adjusted if other securities should be read into database
# query asset_class_id
query = """SELECT id FROM securities_master.asset_class WHERE asset_class = 'stocks';"""
value = pd.read_sql_query(query, engine)
asset_class_id = value.id[0].astype(int)

# definition of the vendor
# need to be adjusted if other securities should be read into database
# query data_vendor_id
query = """SELECT id FROM securities_master.data_vendor WHERE name = 'Yahoo';"""
value = pd.read_sql_query(query, engine)
data_vendor_id = value.id[0].astype(int)


#print('get data from Quandl...')
#data = quandl.get_table("SHARADAR/TICKERS", paginate=True)

print('get ticker data for Yahoo...and fill security / ticker table ')
# download price data and read into memonry
    
path = '/Users/carsten/Documents/Python/Database_PLUS/Data/yahoo_symbols.csv'
data = pd.read_csv(path)


# sending the ticker information to the security table
insert_init = """insert into security 
              (ticker, name, code, sector, isdelisted, ttable, category, exchange_id, asset_class_id, data_vendor_id)
              values """

# get the exchange and exchange_id relation
name_ex_id=get_name_exchange_id()


for index, row in tqdm(data.iterrows(), total=data.shape[0]):
#for index, row in data.iterrows():

    
    if name_ex_id.loc[name_ex_id['name'] == row.exchange ].empty:
        print ("""please add ("{}") to exchange list""".format(row.exchange) )

    
    # find the exchange_id
    exchange_id=name_ex_id.loc[name_ex_id['name'] == row.exchange ].id.iloc[0]
    
    if math.isnan(exchange_id):
        print('error, exchange not in database')
        print(row.exchange)
            
    
    # Add values for all days to the insert statement
    vals = """("{}","{}","{}","{}","{}","{}","{}",{},{},{})""".format(
            row.ticker,
            row['name'],
            row.siccode,
            row.sicsector,
            row.isdelisted,
            row.table,
            row.category,
            exchange_id,
            asset_class_id,
            data_vendor_id)
    
    if index == 0:
        all_vals=vals
    else:
        all_vals= ",".join([all_vals,vals])

# Handle duplicates - Avoiding errors if you've already got some data
# in your table
insert_end = """  on duplicate key update
            ticker        =values(ticker),
            name          =values(name),
            code          =values(code),
            sector        =values(sector),
            isdelisted    =values(isdelisted),
            ttable        =values(ttable),
            category      =values(category),
            exchange_id   =values(exchange_id),
            asset_class_id=values(asset_class_id),
            data_vendor_id=values(data_vendor_id)
            ;"""

# Put the parts together
query = insert_init + all_vals + insert_end
    

# Fire insert statement
engine.execute(query)
Process=True
      
print('ticker table filled')




###################################### populate price table  ###########     populate dividents table

# exchange_id, vendor_id and asset_class_id relation is already stored in the security_id

query_result_df = get_symbol_security_id_quandl('Y1',engine)

for index, row in tqdm(query_result_df.iterrows(), total=query_result_df.shape[0]): 
    symbol          = row.ticker
    security_id   = row.id
    
    # get price from Yahoo
    modus="4"
     
    tick = yf.Ticker(symbol)
        
    # fill from lats date in DB
    if modus=="1":
        last_date=last_import_date(symbol)  
        data = tick.history(start=last_date)
    # fill all 
    elif modus=="2":  
        data = tick.history(period="max")
        
    # fill last period, valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
    elif modus=="3":
        data = tick.history(period="10y")
        
    # fill specific times 
    elif modus=="4":
        data = tick.history(start="1999-01-01", end="2020-02-01")
    else:
        print("Wrong Modus")
    
    # CLEANING DATA -> exchange with 0, NOT OPTIMAL, better would be the mean of the next 5-10 values
    # first replace inf with nan, then fill all nan with 0
    data = data.replace([np.inf, -np.inf], np.nan)
    data = data.fillna(0)

    #data['Ticker']=symbol
    data['Splits'] = data['Stock Splits']
    data = data[['Open','High','Low','Close','Volume','Dividends','Splits']]   

    if not data.empty:

        # send the prices to the daily_price table
        # Add values for all days to the insert statement

        insert_init = """insert into daily_price 
              (trade_date, open, high, low, close, volume , security_id)
              values """

        vals = ",".join(["""('{}',{},{},{},{},{},{})""".format (
            str(index),
            row.Open,
            row.High,
            row.Low,
            row.Close,
            row.Volume,
            security_id ) for index, row in data.iterrows()]) 
    
        insert_end = """  on duplicate key update
            trade_date  =values(trade_date),
            open        =values(open),
            high        =values(high),
            low         =values(low),
            close       =values(close),
            volume      =values(volume),
            security_id =values(security_id)
            ;"""

        # Put the 3 query parts together   
        query = insert_init + vals + insert_end
        
        # Fire insert statement
        engine.execute(query)
        Process=True



        # send the dividends to the dividends table
        insert_init = """insert into dividends 
                 (date, dividends, security_id) values """
    
        # Add values for all days to the insert statement
        vals = ",".join(["""('{}',{},{})""".format (
            str(index),
            row.Dividends,
            security_id ) for index, row in data.iterrows()]) 
               
        insert_end = """  on duplicate key update
                date        =values(date),
                dividends   =values(dividends),
                security_id =values(security_id)
                ;"""

        # Put the 3 query parts together   
        query = insert_init + vals + insert_end

        # Fire insert statement
        engine.execute(query)
        Process=True


        # send the prices to the daily_price table
        insert_init = """insert into corp_action 
                 (date, action, value, security_id) values """

        action='split'         
    
        # Add values for all days to the insert statement
        vals = ",".join(["""('{}','{}',{},{})""".format (
            str(index),
            action,
            row.Splits,
            security_id ) for index, row in data.iterrows()]) 
            
    
        insert_end = """  on duplicate key update
                date         =values(date),
                action       =values(action),
                value        =values(value),
                security_id  =values(security_id)
                ;"""

        # Put the 3 query parts together   
        query = insert_init + vals + insert_end

        # Fire insert statement
        engine.execute(query)
        Process=True



    else:
        if update_DB == False:
            # don't print that message for update==True, as a lot of the ticker are delisted    
            print("""error, no price data for ("{}") found""".format(tik) )


        
print('daily_price table filled  and dividends table filled')





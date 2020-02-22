import pandas as pd

import time
import math
from sqlalchemy import create_engine

from sqlalchemy import create_engine 
from tqdm import tqdm # Used for progress bar

# create the engine for the database
engine = create_engine('mysql+mysqlconnector://root:root@localhost/securities_master')

def available_stocks():

    # this function creates a list of ticker symbols and security_id / ticker_id from the security table
    # the security_id will later point to the content which will be ingestet
    # the list of ticker symbols and security_id can be a mixed of
    # different data_vendor, exchanges and qtables (need this as quandl offer different tables)
    
    # BE CAREFULL, DO NOT MIX TWO SOURCES WITH THE SAME TICKER, 

    # DEFINE VENDOR, MARKET, ASSET CLASS
    # decide which data_vender you want
    data_vendor1 = 'Quandl'
    data_vendor2 = 'Yahoo'
    
    # decide which markets to trade -> NYSE, NASDAQ
    exchange1= 'NYSE'
    exchange2= 'NASDAQ'
    exchange3= 'INDEX'
    
    # decide which asset class, with Quandl table SFP only stocks are included, just adding index with yahoo
    qtable1= 'SF1'
    qtable2= 'Y1'
    
    # define which data vender to use 
    query = """SELECT id FROM securities_master.data_vendor WHERE name = '{}' ; """.format(data_vendor1)
    value = pd.read_sql_query(query, engine)
    data_vendor_id1 = value.id[0].astype(int)

    query = """SELECT id FROM securities_master.data_vendor WHERE name = '{}' ; """.format(data_vendor2)
    value = pd.read_sql_query(query, engine)
    data_vendor_id2 = value.id[0].astype(int)
    
    # define which markets to use 
    query = """SELECT id FROM securities_master.exchange WHERE name = '{}' ; """.format(exchange1)
    value = pd.read_sql_query(query, engine)
    exchange_id1 = value.id[0].astype(int)

    # define which markets to use
    query = """SELECT id FROM securities_master.exchange WHERE name = '{}' ; """.format(exchange2)
    value = pd.read_sql_query(query, engine)
    exchange_id2 = value.id[0].astype(int)

    # define which markets to use
    query = """SELECT id FROM securities_master.exchange WHERE name = '{}' ; """.format(exchange3)
    value = pd.read_sql_query(query, engine)
    exchange_id3 = value.id[0].astype(int)

    # GET the TICKER SYMBOLS
    # Main query for ticker and ticker_id / security_id
    # query ticker symbols and the ticker_id from the security table
    
    query_1 = """SELECT distinct ticker, id FROM security WHERE """

    query_2 = """  ( ttable = '{}' or ttable =  '{}' ) """.format( qtable1, qtable2)

    query_3 = """ and (data_vendor_id = {} or data_vendor_id )""".format( data_vendor_id1, data_vendor_id2 )

    query_4 = """ and ( exchange_id = {} or exchange_id = {} or exchange_id = {}) """.format( exchange_id1, exchange_id2, exchange_id3)
    
    query_5 = """ and ticker not like '$%' order by ticker """
    
    symbol_query = query_1 + query_2 + query_3 + query_4 + query_5
        
    symbol_df = pd.read_sql_query(symbol_query, engine)

    return symbol_df

################ from here, basicly everything is explained in chapter 24 of
################ Clenow, Andreas. „Trading Evolved: Anyone can Build Killer Trading Strategies in Python.“  
################ except for the dividents and splits i used the example from csvdir.py in the same directory of this code
################ and i needed to query 3 tables to get price, dividents and splits
################ the adjusted close price is bulid inside zipline as the input is the not adjused close and the split
################ just wondering if its better to use the adjused close or do it this way?????


"""
The ingest function needs to have this exact signature,
meaning these arguments passed, as shown below.
"""
def sec_master_q(environ,
                  asset_db_writer,
                  minute_bar_writer,
                  daily_bar_writer,
                  adjustment_writer,
                  calendar,
                  start_session,
                  end_session,
                  cache,
                  show_progress,
                  output_dir):
    
    ticker_id = available_stocks() 
    symbols = ticker_id.ticker


    divs_splits = {'divs': pd.DataFrame(columns=['sid', 'amount',
                                              'ex_date', 'record_date',
                                              'declared_date', 'pay_date']),
                   'splits': pd.DataFrame(columns=['sid', 'ratio',
                                                'effective_date'])}    

    
    # Prepare an empty DataFrame for metadata
    metadata = pd.DataFrame(columns=('start_date',
                                              'end_date',
                                              'auto_close_date',
                                              'symbol',
                                              'exchange'
                                              )
                                     )

    # Check valid trading dates, according to the selected exchange calendar
    sessions = calendar.sessions_in_range(start_session, end_session)
    
    # Get data for all stocks and write to Zipline
    daily_bar_writer.write(
            process_stocks(symbols, sessions, metadata, divs_splits, ticker_id)
            )

    # Write the metadata
    asset_db_writer.write(equities=metadata)
    
    # # Write splits and dividends
    divs_splits['divs']['sid'] = divs_splits['divs']['sid'].astype(int)
    divs_splits['splits']['sid'] = divs_splits['splits']['sid'].astype(int)
    adjustment_writer.write(splits=divs_splits['splits'],
                            dividends=divs_splits['divs'])

    
    
"""
Generator function to iterate stocks,
build historical data, metadata 
and dividend data
"""
#def process_stocks(symbols, sessions, metadata, divs, splits):
def process_stocks(symbols, sessions, metadata, divs_splits, ticker_sec_id):    
    # Loop the stocks, setting a unique Security ID (SID)

    sid = 0
    for symbol in tqdm(symbols):
        sid += 1

        # find the security_id for the symbol / ticker
        security_id = ticker_sec_id.loc[ticker_sec_id.ticker == symbol ].id.iloc[0]  

        # Make a database query
        query =  """SELECT trade_date as date, open, high, low, close, volume 
                      FROM daily_price WHERE security_id = {} order by trade_date """.format(security_id)
            
        dfr_price = pd.read_sql_query(query, engine, index_col='date', parse_dates=['date'])  

        query =  """SELECT date as date, dividends as dividend FROM dividends WHERE
                      security_id = {} order by date """.format(security_id)
        dfr_div = pd.read_sql_query(query, engine, index_col='date', parse_dates=['date']) 


        query =  """SELECT date as date, value as split FROM corp_action WHERE
                     action ='split' and security_id = {} order by date """.format(security_id)
            
        dfr_split = pd.read_sql_query(query, engine, index_col='date', parse_dates=['date']) 

        if not dfr_price.empty:

            df = pd.concat([dfr_price, dfr_div, dfr_split], axis=1) 

            df = df.fillna(0)

            # Check first and last date.
            start_date = df.index[0]
            end_date = df.index[-1]        
        
            # Synch to the official exchange calendar
            df = df.reindex(sessions.tz_localize(None))[start_date:end_date]
        
            # Forward fill missing data
            df.fillna(method='ffill', inplace=True)
        
            # Drop remaining NaN
            df.dropna(inplace=True)    
        
            # The auto_close date is the day after the last trade.
            ac_date = end_date + pd.Timedelta(days=1)
        
            # Add a row to the metadata DataFrame.
            metadata.loc[sid] = start_date, end_date, ac_date, symbol, 'NYSE'
        
            if 'dividend' in df.columns:
                # ex_date   amount  sid record_date declared_date pay_date
                tmp = df[df['dividend'] != 0.0]['dividend']
                div = pd.DataFrame(data=tmp.index.tolist(), columns=['ex_date'])
                div['record_date'] = pd.NaT
                div['declared_date'] = pd.NaT
                div['pay_date'] = pd.NaT
                div['amount'] = tmp.tolist()
                div['sid'] = sid

                divs = divs_splits['divs']
                ind = pd.Index(range(divs.shape[0], divs.shape[0] + div.shape[0]))
                div.set_index(ind, inplace=True)
                divs_splits['divs'] = divs.append(div)

            if 'split' in df.columns:
                #tmp = 1. / df[df['split'] != 1.0]['split']
                tmp = df[df['split'] != 0.0 ]
                tmp = 1. / tmp[tmp['split'] != 1.0 ]['split']
            
                split = pd.DataFrame(data=tmp.index.tolist(),
                                  columns=['effective_date'])
                split['ratio'] = tmp.tolist()
                split['sid'] = sid

                splits = divs_splits['splits']
                index = pd.Index(range(splits.shape[0],
                                    splits.shape[0] + split.shape[0]))
                split.set_index(index, inplace=True)
                divs_splits['splits'] = splits.append(split)
            
            yield sid, df

        else:    
             print("""error, no price data for ("{}") found""".format(symbol) )
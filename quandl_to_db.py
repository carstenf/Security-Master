import time
import math
import pandas as pd
from   sqlalchemy import create_engine
from tqdm import tqdm
import quandl
import zipfile
import timeit

# create the engine for the database
engine = create_engine('mysql+mysqlconnector://root:root@localhost/securities_master')

# input you Quand Api key
quandl.ApiConfig.api_key = 'your key'
quandl.ApiConfig.api_version = '2015-04-09'  

# if update == TRUE, find last date in trade_date collum and download from there until today
# ELSE import all avaiable data from Quandel
update_DB = False

if update_DB == True:
    query = """SELECT MAX(trade_date) AS 'last_date' FROM securities_master.daily_price;"""
    update = pd.read_sql_query(query, engine)
    last_update = update.last_date[0]
    # just to bypass the last date in the collum and starts reading from where i want
    #last_update = '2020-02-04'


def get_symbol_security_id_quandl(qtable,engine):
## get ticker symbol and security_id relation

    # query data_vendor_id from the data_vendor table
    query = """SELECT id FROM securities_master.data_vendor WHERE name = 'Quandl';"""
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

# Define Data Vendors and populate data_vendor table

df_vendor=pd.DataFrame({'names': ['Quandl','yahoo'], 'website': ['www.qandl.com', 'www.yahoo.com']})
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
## next step, need always updates if new ipo's exists
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
query = """SELECT id FROM securities_master.data_vendor WHERE name = 'Quandl';"""
value = pd.read_sql_query(query, engine)
data_vendor_id = value.id[0].astype(int)

print('get ticker data from Quandl...and fill security / ticker table ')
# using quandl.get_table for small tables and quandl.export_table for big ones
data = quandl.get_table("SHARADAR/TICKERS", paginate=True)

# sending the ticker information to the security table
insert_init = """insert into security 
              (ticker, name, code, sector, isdelisted, ttable, category, exchange_id, asset_class_id, data_vendor_id)
              values """

# get the exchange and exchange_id relation
name_ex_id=get_name_exchange_id()

for index, row in tqdm(data.iterrows(), total=data.shape[0]):
    
    # check if empty
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
    
    # write all the data into memory and dump them all to the database to improve speed
    # not possible with price and fundamental table; gets overflow message from database
    # if there is an error regarding overflow from database, remove this and change it accordingly
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


################################################################ fill SP00 Members table

# exchange_id, vendor_id and asset_class_id relation is already stored in the security_id

# get Ticker table from Quandl
print('get SP500 data from Quandl...fill SP00 members table')
# using quandl.get_table for small tables and quandl.export_table for big ones
data = quandl.get_table('SHARADAR/SP500', paginate=True)

# get symbol and security_id from Quandl 
symb_sec_id = get_symbol_security_id_quandl('SEP',engine)

if not data.empty:
    
    # sending the information to the security table
    insert_init = """insert into SP500_const 
              (date, action, ticker, contraticker, security_id) values """
    
    for index, row in tqdm(data.iterrows(), total=data.shape[0]):
                
        if not symb_sec_id.loc[symb_sec_id['ticker'] == row.ticker ].empty:
            
            security_id = symb_sec_id.loc[symb_sec_id['ticker'] == row.ticker ].id.iloc[0]
                       
         # Add values for all days to the insert statement
            vals = """('{}','{}','{}','{}',{})""".format(
             row.date,
             row.action,
             row.ticker,
             row.contraticker,
             security_id
             )      
        
            if index == 0:
                all_vals=vals
            else:
                all_vals= ",".join([all_vals,vals])
                
    
    insert_end = """  on duplicate key update
                date         =values(date),
                action       =values(action),
                ticker       =values(ticker),
                contraticker =values(contraticker),
                security_id  =values(security_id)
                ;"""

    # Put the 3 query parts together   
    query = insert_init + all_vals + insert_end
    
     
    # Fire insert statement
    engine.execute(query)
    Process=True
    
    print('SP500_const table filled')  

else:
    print('error: data from Quandel is empyt')
  

############################################################### fill corporate action

# exchange_id, vendor_id and asset_class_id relation is already stored in the security_id

# Build a list with tickers and security_id from security table 'SEP' 
query_result_df = get_symbol_security_id_quandl('SEP',engine)

print('get actions data from Quandl...and fill corporate action')
# using quandl.get_table for small tables and quandl.export_table for big ones
data_all = quandl.get_table('SHARADAR/ACTIONS', paginate=True)

for index, row in tqdm(query_result_df.iterrows(), total=query_result_df.shape[0]): 
    tik           = row.ticker
    security_id   = row.id
    
    # get price table from Quandl for specific ticker and table
    data=data_all.loc[data_all['ticker'] == tik ]
    # fill nan with 0
    data = data.fillna(0)
    
    if not data.empty:
        
        # send the prices to the daily_price table
        insert_init = """insert into corp_action 
                 (date, action, value, contraticker, security_id) values """
    
        # Add values for all days to the insert statement
        vals = ",".join(["""('{}','{}',{},'{}',{})""".format (
            row.date,
            row.action,
            row.value,
            row.contraticker,
            security_id ) for index, row in data.iterrows()]) 
            
    
        insert_end = """  on duplicate key update
                date         =values(date),
                action       =values(action),
                value        =values(value),
                contraticker =values(contraticker),
                security_id  =values(security_id)
                ;"""

        # Put the 3 query parts together   
        query = insert_init + vals + insert_end

        # Fire insert statement
        engine.execute(query)
        Process=True
    
print('corp_action table filled')  


###################################### populate price table  ###########     populate dividents table

###### this part ist still very very slow, needs around 12 hours to store in database......

# exchange_id, vendor_id and asset_class_id relation is already stored in the security_id

# get symbol and security_id from Quandl 
if update_DB == False:   
# download price data and read into memonry
    print('get price data from Quandl...downloading price date, takes 5 min')
    
    path = '/Users/carsten/Documents/Python/Database_PLUS/Data/SHARADAR_SEP.zip'
    # using quandl.get_table for small tables and quandl.export_table for big ones
    dummy = quandl.export_table('SHARADAR/SEP', filename=path)
    print('unzip and read to memory')
    #write the data to memory
    data_price = pd.read_csv(path)


query_result_df = get_symbol_security_id_quandl('SEP',engine)

for index, row in tqdm(query_result_df.iterrows(), total=query_result_df.shape[0]): 
   
    tik           = row.ticker
    security_id   = row.id

    # get price table from Quandl for specific ticker and table
    if update_DB == True:
        # using quandl.get_table for small tables and quandl.export_table for big ones
        data = quandl.get_table('SHARADAR/SEP', paginate=True, lastupdated={'gte':last_update }, ticker=tik)
    else:     
        # read the data from memory
        data = data_price.loc[data_price['ticker'] == tik ]
    # handle NaN
    data = data.fillna(0)

    if not data.empty:

        # send the prices to the daily_price table
        insert_init = """insert into daily_price 
              (trade_date, open, high, low, close, adj_close, volume , security_id)
              values """
    
        #Add values for all days to the insert statement
        vals = ",".join(["""('{}',{},{},{},{},{},{},{})""".format (
            row.date,
            row.open,
            row.high,
            row.low,
            row.closeunadj,
            row.close,
            row.volume,
            security_id ) for index, row in data.iterrows()]) 
   
        insert_end = """  on duplicate key update
            trade_date  =values(trade_date),
            open        =values(open),
            high        =values(high),
            low         =values(low),
            close       =values(close),
            adj_close   =values(adj_close),
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
            row.date,
            row.dividends,
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

    else:
        if update_DB == False:
            # don't print that message for update==True, as a lot of the ticker are delisted    
            print("""error, no price data for ("{}") found""".format(tik) )
        
print('daily_price table filled  and dividends table filled')


############################################################ populate fundamentals table

###### this part ist as well still very very slow to store in database......

# exchange_id, vendor_id and asset_class_id relation is already stored in the security_id

if update_DB == False:
# download fundamental data and read into memonry
    print('get fundamental data from Quandl...downloading fundamental date, takes 5 min')

    path = '/Users/carsten/Documents/Python/Database_PLUS/Data/SHARADAR_SF1.zip'
    # using quandl.get_table for small tables and quandl.export_table for big ones
    dummy = quandl.export_table('SHARADAR/SF1', filename=path)
    print('unzip and read to memory')
    #write data to memory
    data_funda = pd.read_csv(path)


# Build a list with tickers from security table
query_result_df = get_symbol_security_id_quandl('SF1',engine)

for index, row in tqdm(query_result_df.iterrows(), total=query_result_df.shape[0]):    
    tik           = row.ticker
    security_id   = row.id

    # get price table from Quandl for specific ticker and table
    if update_DB == True:
       # using quandl.get_table for small tables and quandl.export_table for big ones
       result = quandl.get_table('SHARADAR/SF1', paginate=True, lastupdated={'gte':last_update }, ticker=tik)
    else:     
       # read data from memory
       result = data_funda.loc[data_funda['ticker'] == tik ]
    
    # handle NaN
    result = result.fillna(0)
    
    if not result.empty:    
        # copy required dimensions into data 
        # ???? (result.loc[result.dimension == 'MRY' ] and result.[result.dimension == 'MRY' ] gets same result ???)
        #result6 = result.loc[result.dimension == 'MRY' ]
        #result5 = result.loc[result.dimension == 'MRQ']
        #result4 = result.loc[result.dimension == 'MRT']
        #result3 = result.loc[result.dimension == 'ARY']
        result2 = result.loc[result.dimension == 'ARQ' ]
        result1 = result.loc[result.dimension == 'ART' ]
        
        data = pd.concat([result1, result2], ignore_index=True)
    
        # send the prices to the daily_price table       
        insert_init = """insert into fundamental 
            (revenue, cor, sgna, rnd, opex, intexp, taxexp, netincdis, consolinc, netincnci,
             netinc, prefdivis, netinccmn, eps, epsdil, shareswa, shareswadil, capex, ncfbus, ncfinv,
             ncff, ncfdebt, ncfcommon, ncfdiv, ncfi, ncfo, ncfx, ncf, sbcomp, depamor,
             assets, cashneq, investments, investmentsc, investmentsnc, deferredrev, deposits, ppnenet, inventory, taxassets,
             receivables, payables, intangibles, liabilities, equity, retearn, accoci, assetsc, assetsnc, liabilitiesc,
             liabilitiesnc, taxliabilities, debt, debtc, debtnc, ebt, ebit, ebitda, fxusd, equityusd,
             epsusd, revenueusd, netinccmnusd, cashnequsd, debtusd, ebitusd, ebitdausd, sharesbas, dps, sharefactor,
             marketcap, ev, invcap, equityavg, assetsavg, invcapavg, tangibles, roe, roa, fcf,
             roic, gp, opinc, grossmargin, netmargin, ebitdamargin, ros, assetturnover, payoutratio, evebitda,
             evebit, pe, pe1, sps, ps1, ps, pb, de, divyield, currentratio,
             workingcapital, fcfps, bvps, tbvps, price, ticker, dimension, calendardate, datekey,reportperiod,
             lastupdated, security_id) values """
        
                # Add values for all days to the insert statement
        vals = ",".join(["""( {},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},
                              {},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},
                              {},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},
                              {},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},
                              {},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},
                              {},{},{},{},{},'{}','{}','{}','{}','{}',
                              '{}',{})""".format (
                  
                  row.revenue, 
                  row.cor,
                  row.sgna,
                  row.rnd,
                  row.opex,
                  row.intexp,
                  row.taxexp,
                  row.netincdis,
                  row.consolinc,
                  row.netincnci,
                  row.netinc,
                  row.prefdivis,
                  row.netinccmn,
                  row.eps,
                  row.epsdil,
                  row.shareswa,
                  row.shareswadil,
                  row.capex,
                  row.ncfbus,
                  row.ncfinv,
                  row.ncff,
                  row.ncfdebt,
                  row.ncfcommon,
                  row.ncfdiv,
                  row.ncfi,
                  row.ncfo,
                  row.ncfx,
                  row.ncf,
                  row.sbcomp,
                  row.depamor, 
                  row.assets,
                  row.cashneq,
                  row.investments,
                  row.investmentsc,
                  row.investmentsnc,
                  row.deferredrev,
                  row.deposits,
                  row.ppnenet,
                  row.inventory,
                  row.taxassets,
                  row.receivables,
                  row.payables,
                  row.intangibles,
                  row.liabilities,
                  row.equity,
                  row.retearn,
                  row.accoci,
                  row.assetsc,
                  row.assetsnc,
                  row.liabilitiesc,
                  row.liabilitiesnc,
                  row.taxliabilities,
                  row.debt,
                  row.debtc,
                  row.debtnc,
                  row.ebt,
                  row.ebit,
                  row.ebitda,
                  row.fxusd,
                  row.equityusd,
                  row.epsusd,
                  row.revenueusd, 
                  row.netinccmnusd,
                  row.cashnequsd,
                  row.debtusd,
                  row.ebitusd,
                  row.ebitdausd,
                  row.sharesbas,
                  row.dps,
                  row.sharefactor,
                  row.marketcap,
                  row.ev,
                  row.invcap,
                  row.equityavg,
                  row.assetsavg,
                  row.invcapavg,
                  row.tangibles,
                  row.roe,
                  row.roa,
                  row.fcf,
                  row.roic,
                  row.gp,
                  row.opinc,
                  row.grossmargin,
                  row.netmargin,
                  row.ebitdamargin,
                  row.ros,
                  row.assetturnover,
                  row.payoutratio,
                  row.evebitda,
                  row.evebit,
                  row.pe,
                  row.pe1,
                  row.sps,
                  row.ps1,
                  row.ps,
                  row.pb,
                  row.de,
                  row.divyield,
                  row.currentratio,
                  row.workingcapital,
                  row.fcfps,
                  row.bvps,
                  row.tbvps,
                  row.price,
                  row.ticker,
                  row.dimension,
                  row.calendardate,
                  row.datekey,
                  row.reportperiod,
                  row.lastupdated,
                  security_id) for index, row in data.iterrows()]) 
        
        insert_end = """  on duplicate key update
                revenue     =values(revenue), 
                cor         =values(cor),
                sgna        =values(sgna),
                rnd         =values(rnd),
                opex        =values(opex),
                intexp      =values(intexp),
                taxexp      =values(taxexp),
                netincdis   =values(netincdis),
                consolinc   =values(consolinc),
                netincnci   =values(netincnci),
                netinc      =values(netinc),
                prefdivis   =values(prefdivis),
                netinccmn   =values(netinccmn),
                eps         =values(eps),
                epsdil      =values(epsdil),
                shareswa    =values(shareswa),
                shareswadil =values(shareswadil),
                capex       =values(capex),
                ncfbus      =values(ncfbus),
                ncfinv      =values(ncfinv),
                ncff        =values(ncff),
                ncfdebt     =values(ncfdebt),
                ncfcommon   =values(ncfcommon),
                ncfdiv      =values(ncfdiv),
                ncfi        =values(ncfi),
                ncfo        =values(ncfo),
                ncfx        =values(ncfx),
                ncf         =values(ncf),
                sbcomp      =values(sbcomp),
                depamor     =values(depamor),
                assets      =values(assets),
                cashneq     =values(cashneq),
                investments   =values(investments),
                investmentsc  =values(investmentsc),
                investmentsnc =values(investmentsnc),
                deferredrev   =values(deferredrev),
                deposits      =values(deposits),
                ppnenet       =values(ppnenet),
                inventory     =values(inventory),
                taxassets     =values(taxassets),
                receivables   =values(receivables),
                payables      =values(payables),
                intangibles   =values(intangibles),
                liabilities   =values(liabilities),
                equity        =values(equity),
                retearn       =values(retearn),
                accoci        =values(accoci),
                assetsc       =values(assetsc),
                assetsnc      =values(assetsnc),
                liabilitiesc  =values(liabilitiesc),
                liabilitiesnc =values(liabilitiesnc),
                taxliabilities =values(taxliabilities),
                debt           =values(debt),
                debtc          =values(debtc),
                debtnc         =values(debtnc),
                ebt            =values(ebt),
                ebit           =values(ebit),
                ebitda         =values(ebitda),
                fxusd          =values(fxusd),
                equityusd      =values(equityusd),
                epsusd         =values(epsusd),
                revenueusd     =values(revenueusd),
                netinccmnusd   =values(netinccmnusd),
                cashnequsd     =values(cashnequsd),
                debtusd        =values(debtusd),
                ebitusd        =values(ebitusd),
                ebitdausd      =values(ebitdausd),
                sharesbas      =values(sharesbas),
                dps            =values(dps),
                sharefactor    =values(sharefactor),
                marketcap      =values(marketcap),
                ev             =values(ev),
                invcap         =values(invcap),
                equityavg      =values(equityavg),
                assetsavg      =values(assetsavg),
                invcapavg      =values(invcapavg),
                tangibles      =values(tangibles),
                roe            =values(roe),
                roa            =values(roa),
                fcf            =values(fcf),
                roic           =values(roic),
                gp             =values(gp),
                opinc          =values(opinc),
                grossmargin    =values(grossmargin),
                netmargin      =values(netmargin),
                ebitdamargin   =values(ebitdamargin),
                ros            =values(ros),
                assetturnover  =values(assetturnover),
                payoutratio    =values(payoutratio),
                evebitda       =values(evebitda),
                evebit         =values(evebit),
                pe             =values(pe),
                pe1            =values(pe1),
                sps            =values(sps),
                ps1            =values(ps1),
                ps             =values(ps),
                pb             =values(pb),
                de             =values(de),
                divyield       =values(divyield),
                currentratio   =values(currentratio),
                workingcapital =values(workingcapital),
                fcfps          =values(fcfps),
                bvps           =values(bvps),
                tbvps          =values(tbvps),
                price          =values(price),
                ticker         =values(ticker),
                dimension      =values(dimension),
                calendardate   =values(calendardate),
                datekey        =values(datekey),
                reportperiod   =values(reportperiod),
                lastupdated    =values(lastupdated),
                security_id    =values(security_id)
                ;"""

        # Put the 3 query parts together   
        query = insert_init + vals + insert_end

        # Fire insert statement
        engine.execute(query)
        Process=True
    else:
        if update_DB == False:
            # don't print that message for update==True, as a lot of the ticker are delisted     
            print("""error, no fundamental data for ("{}") found""".format(tik) )
    
print('fundamental table filled')



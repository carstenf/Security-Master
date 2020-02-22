# Security-Master
database for securities, with import from Quandl and yahoo and ingest to zipline



securities_master.mwb  -> see JPEG

contains the model for the database. This file should be inported by for exp. MySQLWorkbench.
Open MySQLWorkbench, goto File, than Open Model, than open it.
You see the layout. Than go to Database, than Forward Engineer and press continue util it is created.


                      
quandl_to_db.py 

Is the code to get the data from quandl and import it to the database. Im using the "Sharadar Core US Equities Bundle"
It imports price andfundamental data. The code can actually run in 2 Modes, either updating the database or loading
all existing data for first istallation. Loading the 20years price and fundamental data will take around 12hours. 
(maybe someone could optimise this? :)  )
                      


yahoo_to_db.py  

Is the code to get the data from yahoo and imports it to the database. 
I'm using it to import the Index as the Sharadar Core US Equities Bundle does not have an ETF and Index.
To run this, it needs the file "yahoo_symbols.csv" with the ticker to import.
 
 
 
yahoo_symbols.csv  

ticker list for yahoo_to_db.py                 
                      
 
 
db_to_bundle.py

This is the code to ingest the content of the database to zipline. 
At the moment it only supports price data. 
This code has the same function as csvdir.py or quandl.py.
Please check zipline for usage. 
It will take around 30 min to ingest 20year price data to zipline.

# Security-Master
database for securities, with import from Quandl and yahoo and ingest to zipline

Open a Terminal and type > python generate_db.py

This will generate the database.

or

securities_master.mwb  -> see JPEG

contains the model for the database. This file should be inported by for exp. MySQLWorkbench.
Open MySQLWorkbench, goto File, than Open Model, than open it.
You see the layout. Than go to Database, than Forward Engineer and press continue util it is created.

This will generate the database as well, same result



Open a Terminal and type > python quandl_to_db.py 

Is the code to get the data from quandl and import it to the database. Im using the "Sharadar Core US Equities Bundle"
It imports price andfundamental data. The code can actually run in 3 Modes, either updating the database or loading
all existing data for first istallation from Quandl or disk. Please see header for details. Loading the 20years price and fundamental data will take around 12hours. 
(maybe someone could optimise this? :)  )

If you only have pricing, you can comment line 846 fill_fundamental_data() to avoid trying to read the fundamental data.
(and two other places where the fundamental data get called from Quandl)
                      


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

db_to_bundle_w_yahoo.py

Example how to mix different sources, for example yahoo and Quandl


# Using Funtamental data with zipline:

Install the forked Alpha Compiler (forked to work with Security-Master)
and follow the step explained by Alpha Compiler, see as well http://alphacompiler.com/blog/6/
The load_quandl_sf1.py is already modified.
(Open a Terminal and type > python load_quandl_sf1.py)


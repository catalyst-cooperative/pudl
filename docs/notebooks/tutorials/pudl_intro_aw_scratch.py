
# coding: utf-8

# ## Playing with PUDL
# This notebook is meant to help get you up and running with the PUDL database, so you can play with it!
# 
# ### Importing external code.
# We need to import a bunch of outside code to do our work here.  Sometimes we import entire packages (like `numpy` and `pandas`) and sometimes we just pull in a couple of pieces we need from a particular part of a large package (like `declarative_base`)

# In[1]:

import sys
import os.path
import numpy as np
import pandas as pd
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.url import URL
import psycopg2

try:
    conn = psycopg2.connect("dbname='ferc1' user='catalyst' host='localhost' password=''")
except:
    print("I am unable to connect to the database")

# ### Importing our own code
# We also need to tell Python where to look to find our own code.  It has a list of directories that it looks in, but our little project isn't in that list, unless we add it -- which is what `sys.path.append()` does.  You'll need to change this path to reflect where on your computer the PUDL project folder (which you pull down with `git`) lives.
# 
# Once Python knows to look in the `pudl` project folder, it will let you import `pudl` modules just like any other Python module.  Here we're pulling in the `ferc1` and `pudl` modules from the `pudl` package (which is a directory inside the `pudl` project directory).

# In[2]:

sys.path.append('/home/alana/Dropbox/catalyst/pudl')
sys.path.append('/Users/alana/Dropbox/catalyst/pudl')

from pudl import pudl,ferc1, constants, settings, models, models_ferc1,  models_eia923

#try :
#    if(__IPYTHON__) :
	#get_ipython().magic(u'aimport pudl.pudl')
	#get_ipython().magic(u'aimport pudl.ferc1')
	#get_ipython().magic(u'aimport pudl.constants')
	#get_ipython().magic(u'aimport pudl.settings')
	#get_ipython().magic(u'aimport pudl.models')
	#get_ipython().magic(u'aimport pudl.models_ferc1')
	#get_ipython().magic(u'aimport pudl.models_eia923')
#except NameError :
#    pass

# ### Automatically reloading a work in progress
# Because you're probably going to be editing the Python modules related to PUDL while you're working with this notebook, it's useful to have them get automatically reloaded before every cell is executed -- this means you're always using the freshest version of the module, with all your recent edits.

# In[3]:




# ### Connecting to our databases.
# We have two different databases that we're working with right now.  The FERC Form 1 (`ferc1`) and our own PUDL database (`pudl`). For this software to work, you'll need to have the Postgresql database server running on your computer, and you'll need to have created empty databases to receive the tables and data we're going to create.  On a mac, the easiest Postgres install to get running is probably Postgress.app.  You'll need to fire it up at the command line at least once to create the databases (one called `ferc1` and another called `pudl_sandbox`) and a user named `catalyst` with no password.  This information is stored in the `settings` module if you need to look it up.
# 
# Here are two shortcuts for connecting to the two databases once they're created:

# In[4]:

#pudl_engine  = pudl.pudl.db_connect_pudl()
#ferc1_engine = pudl.ferc1.db_connect_ferc1()

try:
	ferc1_engine = psycopg2.connect("dbname='ferc1' user='catalyst' host='localhost' password=''")
	pudl_engine = psycopg2.connect("dbname='pudl_sandbox' user='catalyst' host='localhost' password=''")

except:
    print("I am unable to connect to the database")


# ### Initializing the FERC Form 1 database
# Now that you've got an empty database, let's put some data in it!  This function initializes the database by reading in the FERC Form 1 database structure from `refyear` and data from `years` (which can eventually be a list of years, but that's not working yet...). In order for this to work, you need to have the FERC Form 1 data downloaded into the data directory. There's a script called `get_ferc1.sh` down in `data/ferc/form1/` that will get it for you if you don't have it.

# In[5]:

pudl.ferc1.init_db(refyear=2015, years=[2015,], ferc1_tables=pudl.constants.ferc1_default_tables)


# ### Initializing the PUDL database
# We can also initialize the PUDL database now. Because we're messing around with it a lot, and playing with re-importing data over and over again, it's not a bad idea to destroy whatever old version of it exists in postgres (with `drop_tables_pudl`) and then re-create the tables anew (with `create_tables_pudl`).
# 
# Then we can actualy initialize some of the data (mostly glue tables connecting plants to utilities, and a few lists of constants like the US States for now) using `pudl.init_db()`. Eventually we'll need to tell this where to pull data from... but since we're not really putting any meaningful data into it yet, the function takes no arguments.

# In[ ]:

pudl.pudl.drop_tables_pudl(pudl_engine)
pudl.pudl.create_tables_pudl(pudl_engine)
pudl.pudl.init_db()


# ### Pulling data out of the database!
# Now we're ready to pull some data out of one of the databases, just to show that it works. `pd.read_sql()` takes an SQL Query and a database connection, and puts the results of the query into a pandas DataFrame you can play with easily.

# In[ ]:

ferc1_fuel = pd.read_sql('''SELECT respondent_id, report_year, plant_name, fuel, fuel_unit, fuel_quantity,                                fuel_avg_heat, fuel_cost_delvd, fuel_cost_burned, fuel_cost_btu, fuel_cost_kwh,                                fuel_generaton                                 FROM f1_fuel WHERE plant_name <> '' AND fuel NOT IN ('Total','') ''',ferc1_engine)


# In[ ]:

ferc1_fuel_pudl = pd.read_sql('''SELECT respondent_id, report_year, plant_name, fuel, fuel_unit, fuel_qty_burned,                                fuel_avg_mmbtu_per_unit, fuel_cost_per_unit_delivered, fuel_cost_per_unit_burned, fuel_cost_per_mmbtu, fuel_cost_per_kwh,                                fuel_mmbtu_per_kwh                                 FROM fuel_ferc1 WHERE plant_name <> '' AND fuel NOT IN ('Total','') ''',pudl_engine)


# ### Examining the data we pulled.
# the `sample()` DataFrame method returns a random sample of records from the DataFrame, which is useful for seeing what kinds of things are in there, without always seeing just the first few records.

# In[ ]:

ferc1_fuel.sample(5)


# In[ ]:

ferc1_fuel_pudl.sample(5)


# In[ ]:




# In[ ]:




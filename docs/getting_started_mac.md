## Here’s a step-by-step guide to getting the PUDL database up and running on Mac OS X:
### 1. Reviewing requirements
For the full list of requirements to install, review [REQUIREMENTS.md](https://github.com/catalyst-cooperative/pudl/blob/master/REQUIREMENTS.md) in the PUDL GitHub repository.
### 2. Installing Anaconda and Python packages
1. Anaconda is a package manager, environment manager and Python distribution that contains many of the packages we’ll need to get the PUDL database up and running. Please select the Python 3.6 Graphical Installer version on this [page](https://www.continuum.io/downloads). You can follow a step by step guide to completing the installation on the Graphical Installer [here](https://docs.continuum.io/anaconda/install/mac-os#macos-graphical-install).
2. Two of the required Python packages are not included in Anaconda so we’ll install those separately using pip. In your terminal window, run `pip install dbfread` to install the dbfread package. Next, run `pip install -U sqlalchemy-postgres-copy` to install the postgres-copy package.

### 3. Setting up PostgresQL
1. Now that we have all the required packages installed, we can install the PostgreSQL database. It’s most straightforward to set up through Postgres.app, which is available [here](http://postgresapp.com/).
2. With PostgreSQL installed, we’ll set up command line access to PostgreSQL. In your terminal window, run `sudo mkdir -p /etc/paths.d &&
echo /Applications/Postgres.app/Contents/Versions/latest/bin | sudo tee /etc/paths.d/postgresapp`. Then close the Terminal window and open a new one for changes to take effect. In your new terminal window, run `which psql` and press enter to verify that the changes took effect.
3. We can now set up our PostgreSQL databases. In your terminal window, run `psql` to bring up the PostgreSQL prompt.
  1. Run `CREATE SUPERUSER catalyst with CREATEDB;` to create the catalyst superuser.
  2. Run `CREATE DATABASE ferc1;` to create the database that will receive data from FERC form 1.
  3. Run `CREATE DATABASE pudl;` to create the PUDL database.
  4. Run `CREATE DATABASE pudl_test;` to create the PUDL test database.
  5. Run `CREATE DATABASE ferc1_test;` to create the FERC Form 1 test database.
  6. Run `\q` to exit the PostgreSQL prompt.
### 4. Setting up the PUDL repository
  1. If you don’t have a GitHub account, you’ll need to create one at [github.com](github.com). Since the database is a public repository, you’ll want to select a free public account (the option reads “Unlimited public repositories for free.”).
  2. Once you’ve created an account and confirmed your email address, you’ll want to download and install the GitHub desktop client at [desktop.github.com](desktop.github.com).
  3. Use your new account credentials to log into the GitHub desktop client and select the option to clone a repository. Then, enter the URL `https://github.com/catalyst-cooperative/pudl`.
### 5. Downloading data from FERC and EIA
Now we’re ready to download the data that we’ll use to populate the database.
  1. In your Terminal window, use `cd` to navigate to the directory containing the clone of the PUDL repository.
  2. Within the PUDL respository, use `cd` to navigate to the `pudl/data` directory.
  3. In the `pudl/data` directory, there’s a file called `get-all-data.sh`. Run `bash get-all-data.sh` to run the script, which will bring data from the web into the `pudl/data/eia`, `pudl/data/eia`, and `pudl/data/eia` directories.
### 6. Initializing the database
Let’s initialize the database! In your Terminal window use `cd` to navigate to the `pudl/scripts` directory. Run `python init_pudl.py` to begin the initialization script.
This script will load all of the data that is currently working - years 2004-2016 for FERC Form 1, 2009-2016 for EIA923, and 2011-2015 for EIA860. The whole process will likely take about 20 minutes, and the database will take up about 2GB of space once it's done.
If you want to just do a small subset of the data to test whether the setup is working, check out the help message on the script by calling python `init_pudl.py -h`.
### 7. Playing with the data
In your Terminal window use cd to navigate to the `pudl/docs/notebooks/tutorials directory`. Then run `jupyter notebook pudl_intro.ipynb` to fire up the introductory notebook. There you’ll find more information on how to begin to play with the data.

Here’s a step-by-step guide to getting the PUDL database up and running on Mac OS X:

1. For the full list of requirements to install, review [REQUIREMENTS.md](https://github.com/catalyst-cooperative/pudl/blob/master/REQUIREMENTS.md) in the PUDL GitHub repository.
2. We’ll start by installing Anaconda, a package manager, environment manager and Python distribution that contains many of the packages we’ll need to get the PUDL database up and running. Please select the Python 3.6 Graphical Installer version on this [page](https://www.continuum.io/downloads). You can follow a step by step guide to completing the installation on the Graphical Installer [here](https://docs.continuum.io/anaconda/install/mac-os#macos-graphical-install).
3. Two of the required Python packages are not included in Anaconda so we’ll install those separately using pip. In your terminal window, copy and paste `pip install` dbfread and press enter to install the dbfread package. Next, copy and paste `pip install -U sqlalchemy-postgres-copy` and press enter to install the postgres-copy package.
4. Now that we have all the required packages installed, we can install the PostgreSQL database. It’s most straightforward to set up through Postgress.app, which is available [here](http://postgresapp.com/).
5. With PostgreSQL installed, we’ll set up command line access to PostgreSQL. In your terminal window, copy and paste `sudo mkdir -p /etc/paths.d &&
echo /Applications/Postgres.app/Contents/Versions/latest/bin | sudo tee /etc/paths.d/postgresapp` and press enter. Then close the Terminal window and open a new one for changes to take effect. In your new terminal window, copy and paste `which psql` and press enter to verify that the changes took effect.
6. We can now set up our PostgreSQL databases. In your terminal window, copy and paste `psql` and press enter - this will bring up the PostgreSQL prompt.
  - Copy and paste `CREATE SUPERUSER catalyst with CREATEDB;` and press enter to create the catalyst superuser.
  - Copy and paste `CREATE DATABASE ferc1;` and press enter to create the database that will receive data from FERC form 1.
  - Copy and paste `CREATE DATABASE pudl;` and press enter to create the PUDL database.
  - Copy and paste `CREATE DATABASE pudl_test;` and press enter to create the PUDL test database.
  - Copy and paste `CREATE DATABASE ferc1_test;` and press enter to create the FERC Form 1 test database.
  - Copy and paste `\q` to exit the PostgreSQL prompt.
7. Next we’ll clone the PUDL repository from GitHub.
  - If you don’t have a GitHub account, you’ll need to create one at [github.com](github.com). Since the database is a public repository, you’ll want to select a free public account (the option reads “Unlimited public repositories for free.”).
  - Once you’ve created an account and confirmed your email address, you’ll want to download and install the GitHub desktop client at [desktop.github.com](desktop.github.com).
  - Use your new account credentials to log into the GitHub desktop client and select the option to clone a repository. Then, enter the URL `https://github.com/catalyst-cooperative/pudl`.
8. Now we’re ready to download the data that we’ll use to populate the database.
  - In your Terminal window, use `cd` to navigate to the directory containing the clone of the PUDL repository.
  - Within the PUDL respository, use `cd` to navigate to the `pudl/data` directory.
  - In the `pudl/data` directory, there’s a file called `get-all-data.sh`. Copy and paste `bash get-all-data.sh` and press enter to run the script, which will bring data from the web into the `pudl/data/eia`, `pudl/data/eia`, and `pudl/data/eia` directories.
9. Let’s initialize the database! In your Terminal window use `cd` to navigate to the `pudl/docs/notebooks/tutorials` directory. Then copy and paste `jupyter notebook pudl_intro.ipynb` to fire up the introductory notebook. There you’ll find more information on how to initialize the database and begin to play with the data.

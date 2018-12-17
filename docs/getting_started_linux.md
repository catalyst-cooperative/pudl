
## Step-by-step guide to setting up the PUDL database Linux:

The directions below were most recently tested on [Ubuntu 18.10 Cosmic
Cuttlefish](https://wiki.ubuntu.com/CosmicCuttlefish/ReleaseNotes). They should
also work on other Debian based Linux distributions.

### 0. Short Version
- Install conda and PUDL's Python dependencies.
- Create a postgres user and required databases.
- Run PUDL setup scripts to download data and load it into postgres.


### 1. Review the requirements
For the full list of requirements to install, review [REQUIREMENTS.md](REQUIREMENTS.md) in the PUDL GitHub repository.

### 2. [Cloning the PUDL repository](https://help.github.com/articles/cloning-a-repository/)
If you don’t have a GitHub account, you’ll want to create one at [github.com](https://github.com). Since PUDL is a public, open source repository, you can select a free public account (the option reads “Unlimited public repositories for free.”). Then, pull down the most recent version of the PUDL software, using this command:
```sh
git clone https://github.com/catalyst-cooperative/pudl.git
```

### 3. Installing Anaconda and Python packages
1. Anaconda is a package manager, environment manager and Python distribution that contains many of the packages we’ll need to get the PUDL database up and running. Please select the most recent Python 3 version on [this page](https://www.anaconda.com/download/#linux). You can follow a step by step guide to completing the installation on the Graphical Installer [here](https://docs.anaconda.com/anaconda/install/linux).
    - If you prefer a more minimal install, [miniconda](https://conda.io/miniconda.html) is also acceptable.
2. Install the required packages in a new conda environment called `pudl`. In a terminal window type:
```sh
conda env create --file=environment.yml
```
If you get an error `No such file or directory: environment.yml`, make sure you're in the `pudl` repository downloaded in step 2.
3. Then activate the `pudl` environment with the command:
```sh
conda activate pudl
```
4. Now install the pudl package from the local directory, using `pip`. This allows you to use the software as if it were a normal package installed from the Python Package Index. Make sure you're in the top level directory of the repository, and run:
```sh
pip install --editable .
```
The `--editable` option keeps `pip` from copying files off to the `site-packages` directory, and just creates references to the current directory.

For more on conda environments see [here](https://conda.io/docs/user-guide/tasks/manage-environments.html).

### 4. Setting up PostgreSQL

These instructions borrow from [Ubuntu's PostgreSQL instructions](https://help.ubuntu.com/community/PostgreSQL).

1. You probably already have postgres installed. On a command line, make sure `which psql` gives some result.
If it doesn't:
```sh
sudo apt install postgresql
```

2. Ubuntu has a user called `postgres` that can access the postgresql configuration. Use this user to make a new `catalyst` user with privileges to create databases and login.
```sh
sudo -u postgres createuser --createdb --login --pwprompt catalyst
```
The system will ask for a password. Pick something without a colon (`:`) and write it down for the next step.

3. Create a file in your home directory called [`.pgpass`](https://www.postgresql.org/docs/current/static/libpq-pgpass.html) and add a login line for the catalyst user.
```sh
touch ~/.pgpass
chmod 0600 ~/.pgpass
echo "127.0.0.1:*:*:catalyst:the_password_you_picked" >> ~/.pgpass
```

4. Before the `catalyst` user can log in to the database, you will need to tell postgres how
to authenticate the user. You do this by editing the postgres 'host based authentication' settings file using `nano` or whatever your favorite text editor is as the superuser:
```
sudo nano /etc/postgresql/<version>/pg_hba.conf
```
where `<version>` is the version of postgres (e.g. 9.6, 10). Edit the lines near the end of the file for the `local` sockets and the localhost connections (127.0.0.1 and localhost):
```
# "local" is for Unix domain socket connections only
local   all             all                                     password
# IPv4 local connections:
host    all             all             127.0.0.1/32            password
host    all             all             localhost               password
# IPv6 local connections:
host    all             all             ::1/128                 password
```

5. Now we need to make a few databases to store the data.
Start by logging in as the catalyst user.
```sh
psql -U catalyst -d postgres -h 127.0.0.1
```
If you have login issues, you can delete the `catalyst`  user with the command `sudo -s dropuser catalyst`. The redo step 2.
Once logged in, create these databases:
```sql
CREATE DATABASE ferc1;
CREATE DATABASE pudl;
-- The test databases are only necessary if you're going to run py.test
CREATE DATABASE ferc1_test;
CREATE DATABASE pudl_test;
-- exit with \q
```


### 5. Download the raw data

Now we’re ready to download the data that we’ll use to populate the database.

1. In your Terminal window, use `cd` to navigate to the directory containing the clone of the PUDL repository.
2. Within the PUDL repository, use `cd` to navigate to the `pudl/scripts` directory.
3. In the `pudl/scripts` directory, there’s a script named `update_datastore.py`. To see information on how to use it, run
```sh
python update_datastore.py --help
```
For example, if you wanted to obtain the FERC Form 1, EIA 923, and EIA 860 data for 2014-2017, you would say:
```sh
python update_datastore.py --sources ferc1 eia923 eia860 --years 2014 2015 2016 2017
```
This will download data from the web and populate a well organized data store in `pudl/data/ferc/form1`, `pudl/data/eia/form923`, and `pudl/data/eia/form860` directories.
If the download fails (e.g. the FTP server times out), this command can be run repeatedly until all the files are downloaded. It will not try and re-download data which is already present locally, unless you use the `--clobber` option. Depending on which data sources, how many years or states you have requested data for, and the speed of your internet connection, this may take minutes to hours to complete, and can consume several GB of disk space.

### 6. Load data into the PUDL database
Once the original raw data have been downloaded and organized by the `update_datastore.py` script, you can populate your database with the PUDL initialization script. Its behavior is controlled by a configuration file.  By default, it looks for a file named `settings.yml` in the same directory as the script, but you may copy that default file and rename it to specify your own database initialization parameters. See the default settings file for more information on the configuration options available. You will need to edit the configuration file to specify the data you want to load -- by default it has everything commented out.

If you created a new configuration file named `local_settings.yml` you would initialize the database with the command:
```sh
python init_pudl.py -f local_settings.yml
```
For information about what data is currently available and working, see the [project status section in the README](https://github.com/catalyst-cooperative/pudl/#project-status).

Depending on which data sources and how many years of data you have requested, the database initialization process will take between tens of minutes and several hours complete. The EPA CEMS dataset in particular is enormous, and loading all available data may require the better part of a day. Compressed on disk CEMS is only about 7 GB, but the database files will take up more than 100 GB of space.

### 7. Playing with the data

At the command line, navigate into the `pudl/docs/notebooks/tutorials` directory. Then run `jupyter notebook pudl_intro.ipynb` to fire up the introductory notebook. There you’ll find more information on how to begin to play with the data. If you
installed miniconda in step 3 above, you may need to `conda install jupyter`. **NOTE: this tutorial notebook is out of date and not yet part of our continuous integration, so it is currently unlikely to work.**

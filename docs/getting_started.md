
# Getting Started with the PUDL Data Pipeline

The directions below were most recently tested on [Ubuntu 19.04 Disco
Dingo](https://wiki.ubuntu.com/DiscoDingo/ReleaseNotes). They should
also work on other Debian based Linux distributions.

## 0. The tl;dr Version
 * Clone the PUDL repo.
 * Install PUDL's Python dependencies using some flavor of conda.
 * Create a postgres user and the required `pudl` and `pudl_test` databases.
 * Use the [`update_datastore.py`](/scripts/update_datastore.py) script to download the public data you're interested in.
 * Use the [`ferc1_to_sqlite.py`](/scripts/ferc1_to_sqlite.py) and [`init_pudl.py`](/scripts/init_pudl.py) scripts to process the raw data and load it into the database you just created..

---
## 1. Review the Requirements
PUDL depends on other open source tools as well as public data sources.

### Python 3.6+
We use the [Anaconda](https://www.anaconda.com/download/) Python 3
distribution, and manage package dependencies using conda environments. See the
PUDL [`environment.yml`](/environment.yml) file at the top level of the
repository for the most up to date list of required Python packages and their
versions. Python 3.6 or later is required.

### A Relational Database
PUDL is currently designed to populate a local [Postgres](https://www.postgresql.org/) relational database (v9.6 or later). However, in the near future (mid-2019) we will be transitioning [away from the database](https://github.com/catalyst-cooperative/pudl/issues/258) as the canonical output, and will begin generating and distributing CSV/JSON based [Tabular Data Packages](https://github.com/catalyst-cooperative/pudl/projects/5) instead.

### The Glorious Public Data
All that software isn't any good without some data! The raw data comes from the
US government.  The [update_datastore.py](/scripts/update_datastore.py)
script efficiently downloads and organizes this data for you, so that PUDL
knows where to find it. Currently the data PUDL can ingest includes:
 - **[EIA Form 860](https://www.eia.gov/electricity/data/eia860/)** (2011-2017) All available data is **~400 MB** on disk and about **half a million** records in the database.
 - **[EIA Form 923](https://www.eia.gov/electricity/data/eia923/)** (2009-2017) All available data is **~500 MB** and **2 million records**.
 - **[EPA CEMS Hourly](https://ampd.epa.gov/ampd/)** (1995-2018) All available data is **~8 GB** compressed, **~100 GB** in the database, with **~1 billion records** that get pulled into PUDL.
 - **[FERC Form 1](https://www.ferc.gov/docs-filing/forms/form-1/data.asp)** (2004-2017) The useful original data for 1994-2017 is **~1 GB** on disk, and **12 million** records, but right now we only pull in **~1 million** records making up less than **100 MB**.

---
## 2. [Clone the PUDL repository](https://help.github.com/articles/cloning-a-repository/)
PUDL has not yet been released via one of the common Python package systems, so you have to download the code directly from GitHub by [cloning the repository](https://help.github.com/articles/cloning-a-repository/) to your own computer. Depending on your platform (Linux, OSX, Windows...) and the way you access GitHub, the exact process will differe. If you don’t have a GitHub account, you’ll probably want to create one at [github.com](https://github.com). PUDL is a public, open source repository, so a free account is all you need.

If you're using a UNIX-like terminal, the command will look like this:
```sh
git clone https://github.com/catalyst-cooperative/pudl.git
```
---
## 3. Install Anaconda and the Required Python packages
#### Install a conda-based Python 3.6+ environment.
Anaconda is a package manager, environment manager and Python distribution that contains almost all of the packages we’ll need to get the PUDL database up and running. Please select the most recent Python 3 version on [this page](https://www.anaconda.com/download/#linux). You can find step by step instructions for the installation [here](https://docs.anaconda.com/anaconda/install/).

If you are familiar with package managers and prefer a more minimal install, [miniconda](https://conda.io/miniconda.html) will also work just fine.

#### Install the required packages in a new conda environment called `pudl`
From the top level of your cloned PUDL repository, in a terminal window type:
```sh
conda env create --file=environment.yml
```
If you get an error `No such file or directory: environment.yml`, make sure you're in the `pudl` repository downloaded in step 2. For more on conda environments see [here](https://conda.io/docs/user-guide/tasks/manage-environments.html).

#### Activate the `pudl` environment.
From within a UNIX-like shell, use the command:
```sh
conda activate pudl
```
Now you should probably see `(pudl)` to the left of your command line prompt, indicating that the environment is active.

#### Install the pudl package locally using `pip`.
The above commands installed the packages that `pudl` depends on, but not `pudl` itself. Until we've released the package to PyPI, you need to install it manually from your clone of the repository. This will allow you to use the PUDL library as if it were a normal package installed from the Python Package Index. Make sure you're in the top level directory of the repository, and run:
```sh
pip install --editable ./
```
The `--editable` option keeps `pip` from copying files into to the `site-packages` directory, and just creates references directly to the current directory (aka `./`).

---
## 4. Install and Configure PostgreSQL
PUDL is currently designed to populate a local [Postgres](https://www.postgresql.org/) relational database (v9.6 or later). Exactly what is required to get the Postgresql database up and running varies considerably from platform to platform, and this step has often been a stumbling block for new users. Partly because of that, in the near future (mid-2019) we will be transitioning [away from the database](https://github.com/catalyst-cooperative/pudl/issues/258) as the canonical output, and will begin generating and distributing CSV/JSON based [Tabular Data Packages](https://github.com/catalyst-cooperative/pudl/projects/5) instead.

### Install PostgreSQL on Debian Based Linux
These instructions borrow from [Ubuntu's PostgreSQL instructions](https://help.ubuntu.com/community/PostgreSQL).

#### Make sure postgresql (version 9.6 or later) is installed.
You probably already have postgresql installed. On a command line, make sure `which psql` gives some result. If it doesn't:
```sh
sudo apt install postgresql
```

#### Create a database user named `catalyst` with appropriate privileges.
Ubuntu has a user called `postgres` that can access the postgresql configuration. Use this user to make a new `catalyst` user with privileges to create databases and login. At the command line this will look like:
```sh
sudo -u postgres createuser --createdb --login --pwprompt catalyst
```
The system will ask for a password. Pick something without a colon (`:`) and write it down for the next step.

#### Create a local [`.pgpass`](https://www.postgresql.org/docs/current/static/libpq-pgpass.html) file to automatically log the `catalyst` user in to the database
Use the following commands:
```sh
touch ~/.pgpass
chmod 0600 ~/.pgpass
echo "127.0.0.1:*:*:catalyst:the_password_you_picked" >> ~/.pgpass
```

#### Tell postgresql to authenticate the `catalyst` user using the `.pgpass` file.
Before the `catalyst` user can log in to the database, you will need to enable local file-based authentication. You can do this by editing the postgres "host based authentication" settings file using `nano`, `emacs`, `vi` or whatever your favorite text editor is as the superuser:
```sh
sudo nano /etc/postgresql/<version>/pg_hba.conf
```
where `<version>` is the version of postgresql (e.g. 9.6, 10). Edit the lines near the end of the file for the `local` sockets and the localhost connections (127.0.0.1 and localhost) to look like this:
```
# "local" is for Unix domain socket connections only
local   all             all                                     password
# IPv4 local connections:
host    all             all             127.0.0.1/32            password
host    all             all             localhost               password
# IPv6 local connections:
host    all             all             ::1/128                 password
```

#### Create an empty PUDL database in postgresql.
Now we need to make a few databases to store the data. Start by logging in as the catalyst user.
```sh
psql -U catalyst -d postgres -h 127.0.0.1
```
If you have login issues, you can delete the `catalyst`  user with the command `sudo -s dropuser catalyst`. Then redo the user creation step above. Once you've logged in successfully, create two databases using these commands:
```sql
CREATE DATABASE pudl;
CREATE DATABASE pudl_test;
-- exit psql with \q
```
### Install and Configure PostgreSQL on Apple's Mac OS X
There are a variety of PostgreSQL distributions available for Mac OS X. We've found that the easiest one for most people is [Postgres.app](http://postgresapp.com/).

#### Download and install Postgres.app
by following [their instructions here](http://postgresapp.com).

#### Start the PostgreSQL server by opening Postgres.app.
After installing Postgres.app, open the application by double clicking on the blue elephant. The application always needs to be running when you are working with PUDL -- you can either add it to your Startup Items (under Settings), or start it manually whenever you are going to use PUDL.

#### Set up command line access to Postgres
In your Terminal window, run:
```sh
sudo mkdir -p /etc/paths.d && echo /Applications/Postgres.app/Contents/Versions/latest/bin | sudo tee /etc/paths.d/postgresapp
```
Then close the Terminal window and open a new one for changes to take effect. In your new terminal window, type `which psql` and press enter to verify that the changes took effect.

#### Use `psql` to set up empty PUDL databases
In a terminal window, run `psql` to bring up the PostgreSQL prompt.
```sql
CREATE USER catalyst with CREATEDB;
CREATE DATABASE pudl;
CREATE DATABASE pudl_test;
-- exit psql with \q
```

### Install and Configure PostgreSQL on Microsoft Windows
#### [Download the PostgreSQL installer](https://www.postgresql.org/download/windows/)
The EnterpriseDB version is fine; you don't need the extras included in BigSQL. Install the latest PostgreSQL version (currently 11) for Windows.

#### Install PostgreSQL
The installer requires you to set a password for the `postgres` user. Remember this password. In the installation, you should install both the **PostgreSQL Server** and the **pgAdmin 4 Client**. The installer offers other things, like Stack Builder, that aren't necessary.

#### Set up the PUDL PostgreSQL databases
  1. Open the pgAdmin 4 program and log in to the `postgres` account.
  - Right click "Login/Group Roles" and add a new user called `catalyst`.
  - Set a password for the `catalyst` user.
  - Grant the `catalyst` user permissions to login and create databases.
  - Next, right-click on "databases" and open up the create database menu.
  - Create the `pudl` database with `catalyst` as the owner.
  - Repeat the last step to create a database called `pudl_test`.


#### Set up database authentication
Create a `pgpass.conf` file with the password you set using the following process:
  1. In Windows Explorer, type %APPDATA%\postgresql into the address bar and press enter. This should take you to something like c:\users\username\appdata\local\postgresql, but don't worry if that's not the exact path.
  - See if there's already a file called `pgpass.conf`.
  - If there is, open it. If not, use Notepad to create a text file called `pgpass.conf` in that folder.
  - Whether it previously existed or not, you're going to add a new line. The contents of that line will be:
```
127.0.0.1:*:*:catalyst:the_password_you_picked
```
  (substituting in the password you actually picked for the catalyst user).
  - The line above says "whenever you try to connect to the local machine over IPv4 with the username `catalyst`, use the password &lt;the_password_you_picked&gt;.
  - Save and close.

---
## 5. Download the Raw Public Data.
Now you’re ready to download the raw data that gets fed into the PUDL data processing pipeline, and used to populate the database you just created.

In your Terminal window, use `cd` to navigate to top level directory of the cloned PUDL repository. If you `ls` there you should see directories named `ci`, `data`, `docs`, `pudl`, `results`, `test` the `environment.yml` file, etc.

Remember that in order to use the data downloading script (and the rest of PUDL) you need to activate the `pudl` conda environment that you created earlier. If you haven't already done so in your current terminal, make sure you run:
```sh
conda activate pudl
```
Now navigate into the scripts directory:
```sh
cd pudl/scripts
```
If you `ls` in the `pudl/scripts` directory you should see a script named `update_datastore.py`. To see information on how to use it, run:
```sh
python update_datastore.py --help
```
If you get an error here like `no module named pudl`, or `SyntaxError: invalid syntax` make sure you've both installed the `pudl` python package locally, and that the `pudl` conda environment is activated. See above for instructions.

### Example usage for `update_datastore.py`
If you want to obtain the FERC Form 1, EIA 923, and EIA 860 data for 2014-2017, you would say:
```sh
python update_datastore.py --sources ferc1 eia923 eia860 --years 2014 2015 2016 2017
```
Or you can update one data source and year at a time if you prefer:
```sh
python update_datastore.py -s eia923 -y 2016
python update_datastore.py -s eia860 -y 2016
python update_datastore.py -s eia923 -y 2017
python update_datastore.py -s eia860 -y 2017
```

To get all the available years of EPA CEMS Hourly data for the western US you'd say:
```sh
python update_datastore.py --sources epacems --states CA OR WA AZ NV ID UT NM CO WY MT
```
The downloaded data will be used by the script to populate a data store under the `data` directory, organized by data source, form, and date:
  * `pudl/data/eia/form860/`
  * `pudl/data/eia/form923/`
  * `pudl/data/epa/cems/`
  * `pudl/data/ferc/form1/`


If you download all of the available data, it may take a long time, especially the EPA CEMS Hourly. Approximate sizes for the various datasets when compressed for download, stored uncompressed on disk, and in terms of the approximate number of records pulled in to PUDL:

| Data Source                                                               |   Years   | Download | On Disk | # of Records |
|---------------------------------------------------------------------------|-----------|----------|---------|--------------|
| **[EIA Form 860](https://www.eia.gov/electricity/data/eia860/)**          | 2001-2017 | 100 MB   | 400 MB  | ~500,000     |
| **[EIA Form 923](https://www.eia.gov/electricity/data/eia923/)**          | 2001-2017 | 200 MB   | 500 MB  | ~2 million   |
| **[EPA CEMS](https://ampd.epa.gov/ampd/)**                                | 1995-2018 | 10 GB    | 100 GB  | ~1 billion   |
| **[FERC Form 1](https://www.ferc.gov/docs-filing/forms/form-1/data.asp)** | 1994-2017 | 1 GB     | 10 GB   | ~1 million   |

If the download fails (e.g. the FTP server times out), this command can be run repeatedly until all the files are downloaded. It will not try and re-download data which is already present locally, unless you use the `--clobber` option. Depending on which data sources, how many years or states you have requested data for, and the speed of your internet connection, this may take minutes to hours to complete, and can consume 20+ GB of disk space.

----
## 6. Run the PUDL Pipeline
Once the original raw data have been downloaded and organized by the `update_datastore.py` script, you are ready to run the data processing pipeline. This set of operations converts the huge pile of FoxPro database files, CSVs, and Excel spreadsheets into a clean, usable relational database. The pipeline performs 3 basic steps:
* **Extract** the data from its original source formats and into Pandas dataframes where we can easily manipulate it.
* **Transform** the extracted data into tidy tabular data structures, applying a variety of cleaning routines, and creating connections both within and between the various datasets.
* **Load** the data into the database, or another accessible, platform-independent interchange format like [Tabular Data Packages](https://frictionlessdata.io/specs/tabular-data-package/).

### Clone the FERC Form 1 Database
FERC Form 1 is special. It is published in a particularly inaccessible format (binary FoxPro database files), and the data itself is particularly unclean and poorly organized. As a result, very few people are currently able to make use of it at all, and we have not yet integrated the vast majority of the available data into PUDL. Also as a result, there is significant value in simply providing programmatic access to the bulk raw data, in addition to the cleaned up subset of the data within PUDL.

In order to provide that access, we've broken the FERC Form 1 `extract` step into two separate parts: cloning the *entire* original database from FoxPro into SQLite (116 distinct tables, with thousands of fields, covering the time period from 1994 to the present), and then pulling a subset of the data out of [SQLite](https://www.sqlite.org/) for further processing. If you want direct access to the full uncleaned FERC Form 1 database, you can just do the database cloning, and connect directly to the SQLite database with your favorite modern tools. This is particularly useful now, as Microsoft has discontinued the database driver that until late 2018 had allowed users to load the FoxPro database files into Microsoft Access.

In any case, cloning the original database is the first step in the PUDL ETL process.

If you have a fully populated datastore, which includes the FERC Form 1 data, all you have to do is go into the `scripts` directory and run:

```sh
python ferc1_to_sqlite.py settings_ferc1_to_sqlite_default.yml
```

This will create a file-based SQLite database that you can find at `results/sqlite/ferc1.sqlite` By default, the script pulls in all available years of data, and all but 3 of the 100+ database tables. The excluded tables (`f1_footnote_tbl`, `f1_footnote_data` and `f1_note_fin_stmnt`) contain unreadable binary data, and increase the overall size of the database by a factor of ~10 (to 7.2 GB rather than 768 MB). If for some reason you need access to those tables, you can create a copy of the settings file and un-comment those tables in the list of tables that it directs the script to load.

Note that this script pulls *all* the data into a single database, rather than keeping it split out by year, as in the original data as distributed. Virtually all the database tables contain a `report_year` column that indicates which year they came from. One notable exception is the `f1_respondent_id` table, which maps `respondent_id`'s to the names of the respondents. For that table, we have allowed the most recently reported record to take precedence.

The FERC Form 1 database is not particularly... relational. The only foreign key relationships that exist map `respondent_id` fields in the individual data tables back to `f1_respondent_id`. In theory, most of the data tables use `report_year`, `respondent_id`, `row_number`, `spplmnt_num` and `report_prd` as a composite primary key (at least, according to [this design document we found](/docs/ferc/form1/FERC_Form1_Database_Design_Diagram_2015.pdf)). However in practice, there are several thousand records (out of ~12 million), including some in almost every database table, that violate the uniqueness constraint on those primary keys (which are apparently not strictly enforced by FoxPro?). Given the lack of meaningful foreign key relationships, we chose to preserve all of the records and use surrogate autoincrementing primary keys in the cloned SQLite database.

### Initialize the PUDL Database
Once the FERC Form 1 database has been cloned, you can use another script to pull all the data together in the PUDL PostgreSQL database.

The script is controlled by a human readable configuration file written in YAML. We provide an example in `[scripts/settings_init_pudl_default.yml](/scripts/settings_init_pudl_default.yml)`. You should make a copy of the file, maybe called something like `settings_init_pudl_local.yml` and modify it to suit your purposes -- including the years and data sources that you need to work with.  Or, if you have plenty of time and space, you can just tell it to pull in everything. However, be warned that the EPA CEMS Hourly dataset is ~100 GB uncompressed, and will typically take hours to load.

To initalize the PUDL database, go into the `scripts` directory and type:

```sh
python init_pudl.py my_local_settings_file.yml
```
(assuming you named your local settings file `my_local_settings_file.yml`). The script will generate output to let you know what its doing.

For information about what data is currently available and working, see the [project status section in the README](https://github.com/catalyst-cooperative/pudl/#project-status).

Depending on which data sources and how many years of data you have requested, the database initialization process will take between a few minutes and several hours.

## 7. Accessing the Data

Once `init_pudl.py` finishes running, you have a live PostgreSQL database named `pudl` which you can access as the `catalyst` user that you set up above, via whatever tools you're most familiar with. We primarily use interactive [Jupyter notebooks](https://jupyter.org/), and there are several example notebooks included in the repository that should be tested and working. Any notebook found in either of these directories should work:
 * `[docs/notebooks/](/docs/notebooks)`
 * `[test/notebooks/](/test/notebooks)`

At the command line, you can start a local Jupyter Notebook server and access these notebooks. Make sure that you have the `pudl` conda environment activated in the top level of the repository and then launch one of the notebooks like this:

```sh
jupyter-notebook docs/notebooks/pudl_intro.ipynb
```
or, if you would rather use the newer JupyterLab interface you can do:
```sh
jupyter-lab
```
and then use the file browser on the left hand side to navigate down into the
`[docs/notebooks](/docs/notebooks)` or `[test/notbooks](/test/notebooks)` directories to pick a notebook to play with or create your own notebook,
using one of ours as a template. In addition to accessing the data within the
PUDL database, these notebooks show you how to access some of analyses that
we've built on top of the PUDL database, such as estimating the marginal cost
of electricity from fuel on a per-generator basis.

## 8. Contributing
If you've made it this far, congratulations! Hopefully you've now got gigabytes of US energy system data at your fingertips! We would love to get your suggestions and feedback. For instance you could...
 * Check out our [Code of Conduct](/docs/CODE_OF_CONDUCT.md)
 * File a [bug report](https://github.com/catalyst-cooperative/pudl/issues/new?template=bug_report.md) if you find something that's broken.
 * Make a [feature request](https://github.com/catalyst-cooperative/pudl/issues/new?template=feature_request.md) if you think there's something we should add.
 * Email us at [pudl@catalyst.coop](mailto:pudl@catalyst.coop)
 * Chat with us on [Gitter](https://gitter.im/catalyst-cooperative/pudl).
 * Ask for an invite to [our Slack](https://catalystcooperative.slack.com/).
 * Sign up for our (irregular, infrequently published) [e-mail newsletter](https://catalyst.coop/updates/).
 * Follow [@CatalystCoop on Twitter](https://twitter.com/CatalystCoop)

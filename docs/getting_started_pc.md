
## Here’s a step-by-step guide to getting the PUDL database up and running on Windows:

### 0. Short Version
- Install conda and python dependencies
- Create postgres user and databases
- Run PUDL setup scripts to download data and load into postgres

### 1. Reviewing requirements
For the full list of requirements to install, review [REQUIREMENTS.md](https://github.com/catalyst-cooperative/pudl/blob/master/REQUIREMENTS.md) in the PUDL GitHub repository.

### 2. Setting up the PUDL repository
1. [Clone](https://help.github.com/articles/cloning-a-repository/) the PUDL repository. (This may require installing Git if you don't already have it.)
```sh
git clone git@github.com:catalyst-cooperative/pudl.git
```


### 3. Installing Anaconda and Python packages
1. Anaconda is a package manager, environment manager and Python distribution that contains many of the packages we’ll need to get the PUDL database up and running. Please select the Python 3.6 version on this [page](https://www.anaconda.com/download/#windows). You can follow a step by step guide to completing the installation on the Graphical Installer [here](https://docs.anaconda.com/anaconda/install/windows).
    - If you prefer a more minimal install, [miniconda](https://conda.io/miniconda.html) is also acceptable.
2. Set up conda to use [conda-forge](https://conda-forge.org/), and install the required packages. In a terminal window type:
```sh
conda config --add channels conda-forge
# To install in the default conda environment:
#conda install --file=docs/requirements_conda.txt
# Or to install in a new environment called 'pudl'
conda create -n pudl --file=docs/requirements_conda.txt
```
More on conda environments [here](https://conda.io/docs/user-guide/tasks/manage-environments.html).

3. One of the required Python packages are not included in conda-forge so we’ll install it separately using pip. In your terminal window, run the following command to install the `postgres-copy` package.
```sh
pip install sqlalchemy-postgres-copy==0.5.0
```

### 4. Setting up PostgreSQL


1. [Download](https://www.postgresql.org/download/windows/) the Postgres installer.
The EnterpriseDB version is fine; you don't need the extras included in BigSQL.
Install the latest PostgreSQL version (currently 10.2) for Windows (32- or 64-bit).

2. The installer requires you to set a password for the `postgres` user.
Remember this.
In the installation, do install you should install PostgreSQL server and pgAdmin 4.
The installer offers other things, like Stack Builder, that aren't necessary.

3. We can now set up our PostgreSQL databases. Open the pgAdmin 4 program.


4. In pgAdmin 4, log into the `postgres` account.
5. Right click "Login/Group Roles" and add a new user called `catalyst`. Set a password for the `catalyst` user.
    - Grant the catalyst users permissions to login and create databases.
6. Next, right-click on "databases" and open up the create database menu.
Create the `ferc1` database with `catalyst` as the owner. This database will receive data from FERC form 1.
7. Repeat #6 to create databases called `pudl`, `ferc1_test`, and `pudl_test`.
8. Set up a `pgpass.conf` file with the password you set:
    - In Windows Explorer, type %APPDATA%\postgresql into the address bar and press enter. This should take you to something like c:\users\username\appdata\local\postgresql, but don't worry if that's not the exact path.
    - See if there's already a file called pgpass.conf
    - If there is, open it. If not, use Notepad to create a text file called pgpass.conf in that folder.
    - Whether it previously existed or not, you're going to add a new line.
    - The contents of that line will be: `127.0.0.1:*:*:catalyst:the password you picked`, substituting in the password you picked for the catalyst user.
        - The line above says "whenever you try to connect to the local machine over IPv4 with the username `catalyst`, use the password ______".
    - Save and close.


### 5. Initializing the database

Now we’re ready to download the data that we’ll use to populate the database.

1. In your Terminal window, use `cd` to navigate to the directory containing the clone of the PUDL repository.
2. Within the PUDL repository, use `cd` to navigate to the `pudl\scripts` directory.
3. In the `pudl\scripts` directory, there’s a file called `update_datastore.py`. Run this with
```sh
python update_datastore.py
```
This will bring data from the web into the `pudl\data\eia`, `pudl\data\eia`, and `pudl\data\eia` directories.
4.  Once the datasets are downloaded and unzipped by `update_datastore.py`, begin the initialization script with
```sh
python init_pudl.py
```
This script will load all of the data that is currently working (see [README.md](https://github.com/catalyst-cooperative/pudl/#project-status) for details.)
5. This process will take tens of minutes to download the data and about 20 minutes to run the initialization script. The unzipped data folder will be about 8GB and the postgres database will take up about 1GB.
If you want to just do a small subset of the data to test whether the setup is working, check out the help message on the script by calling python `init_pudl.py -h`.

### 6. Playing with the data

In your Terminal window use cd to navigate to the `pudl\docs\notebooks\tutorials directory`. Then run `jupyter notebook pudl_intro.ipynb` to fire up the introductory notebook. There you’ll find more information on how to begin to play with the data.
(If you installed miniconda in step 2, you may have to `conda install jupyter`.)

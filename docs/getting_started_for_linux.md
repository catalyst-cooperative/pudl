
## Here’s a step-by-step guide to getting the PUDL database up and running on Linux:

The directions below were written for Ubuntu 17.10.
They may work on other operating systems too.

### 0. Short Version
- Install conda and python dependencies
- Create postgres user and databases
- Run PUDL setup scripts to download data and load into postgres


### 1. Reviewing requirements
For the full list of requirements to install, review [REQUIREMENTS.md](https://github.com/catalyst-cooperative/pudl/blob/master/REQUIREMENTS.md) in the PUDL GitHub repository.

### 2. Installing Anaconda and Python packages
1. Anaconda is a package manager, environment manager and Python distribution that contains many of the packages we’ll need to get the PUDL database up and running. Please select the Python 3.6 version on this [page](https://www.anaconda.com/download/#linux). You can follow a step by step guide to completing the installation on the Graphical Installer [here](https://docs.anaconda.com/anaconda/install/linux).
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

### 3. Setting up PostgreSQL

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
The system will ask for a password. Pick something without a colon (`:`) or  and remember it until the next step.

3. Create a file in your home directory called [`.pgpass`](https://www.postgresql.org/docs/current/static/libpq-pgpass.html) and add a login line for the catalyst user.
```sh
touch ~/.pgpass
chmod 0600 ~/.pgpass
echo "127.0.0.1:*:*:catalyst:the_password_you_picked" >> ~/.pgpass
```

4. Now we need to make a few databases to store the data.
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



### 4. Setting up the PUDL repository
1. [Clone](https://help.github.com/articles/cloning-a-repository/) the PUDL repository. (This may require installing Git if you don't already have it.)
```sh
git clone git@github.com:catalyst-cooperative/pudl.git
```


### 5. Initializing the database

Now we’re ready to download the data that we’ll use to populate the database.

1. In your Terminal window, use `cd` to navigate to the directory containing the clone of the PUDL repository.
2. Within the PUDL repository, use `cd` to navigate to the `pudl/scripts` directory.
3. In the `pudl/scripts` directory, there’s a file called `update_datastore.py`. Run this with
```sh
python update_datastore.py
```
This will bring data from the web into the `pudl/data/eia`, `pudl/data/eia`, and `pudl/data/eia` directories.
4.  Once the datasets are downloaded and unzipped by `update_datastore.py`, begin the initialization script with
```sh
python init_pudl.py
```
This script will load all of the data that is currently working (see [README.md](https://github.com/catalyst-cooperative/pudl/#project-status) for details.)
5. This process will take tens of minutes to download the data and about 20 minutes to run the initialization script. The unzipped data folder will be about 8GB and the postgres database will take up about 1GB.
If you want to just do a small subset of the data to test whether the setup is working, check out the help message on the script by calling python `init_pudl.py -h`.

### 7. Playing with the data

In your Terminal window use cd to navigate to the `pudl/docs/notebooks/tutorials directory`. Then run `jupyter notebook pudl_intro.ipynb` to fire up the introductory notebook. There you’ll find more information on how to begin to play with the data.
(If you installed miniconda in step 2, you may have to `conda install jupyter`.)

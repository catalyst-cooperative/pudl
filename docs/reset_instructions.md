# Resetting the PUDL database

The steps below are sorted in operation order.
If you are experiencing errors about dropping tables, the second step, "resetting the databases"


## Re-downloading the data

Run the `update_datastore.py` script, located in the `scripts` folder, with the `--clobber` flag.

For example, to re-download and re-unzip all the datasets, run:
```sh
python update_datastore.py --clobber
```
To re-download only one dataset, specify a data source. For example, to reload the EIA form 923 data, you would run:
```sh
python update_datastore.py --clobber --sources eia923
```

To re-unzip the data without re-downloading, run:
```sh
python update_datastore.py --clobber --no-download
```

For more information on the `update_datastore.py` script, run:
```sh
python update_datastore.py --help
```


## Resetting the databases

Resetting the databases has two steps:

1. Drop and recreate the databases
2. Reload the data

Dropping and recreating the databases can be done on the command line with the `psql` client, or with a graphical interface like pgAdmin.


Here are commands you could run to drop and recreate the `pudl`, `ferc`, `pudl_test`, and `ferc1_test` databases.
If you are not testing the PUDL code, you do not need the last four lines.
```sh
psql -U catalyst -d pudl_test -c 'DROP DATABASE pudl'
psql -U catalyst -d pudl_test -c 'DROP DATABASE ferc1'
psql -U catalyst -d pudl_test -c 'CREATE DATABASE pudl'
psql -U catalyst -d pudl_test -c 'CREATE DATABASE ferc1'

# These are only useful if you are testing pudl with py.test:
psql -U catalyst -d pudl -c 'DROP DATABASE pudl_test'
psql -U catalyst -d pudl -c 'DROP DATABASE ferc1_test'
psql -U catalyst -d pudl -c 'CREATE DATABASE pudl_test'
psql -U catalyst -d pudl -c 'CREATE DATABASE ferc1_test'
```

Next, whatever procedure you used to drop the and re-create the databases, you need to run the `init_pudl.py` script.
Within the `scripts` folder, run:
```sh
python init_pudl.py
```
It should print out the datasets as it processes them.  Once it's done, you're all set!

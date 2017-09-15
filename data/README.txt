The data associated with PUDL is too large for easy storage within the
Git repository. We have provided a python module for doing datastore
management, including downloading the original files from the public agencies
which compile the data, and a script which uses that module to initialize and
update the datastore as needed

../pudl/datastore.py
../scripts/update_datastore.py

For more information, cd into the scripts directory and run:

python update_datastore.py --help

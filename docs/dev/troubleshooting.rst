===============================================================================
Development Troubleshooting
===============================================================================

Having trouble working on a ticket or with a new asset? Look below for 
recommendations on how to handle specific issues that might pop up. (Note that
this list is evolving over time.)

-------------------------------------------------------------------------------
Extracting all years of data from a Zenodo archive
-------------------------------------------------------------------------------

Let's say you're working with raw data from PUDL's :ref:`Zenodo archive
<access-stable>`. You downloaded the raw data and now you want to materialize
all years available in the archive. In this case, you need to make sure to
materialize the asset group - not just the relevant "...__all_dfs" asset.

-------------------------------------------------------------------------------
Catch up to the current state of PUDL
-------------------------------------------------------------------------------

Maybe you haven't worked on PUDL for a while or you're working on a ticket but
you want to make sure that your version of the repo is up-to-date with the
production version. Once you've activated your local environment, run the
following commands to make sure you have the latest code:

.. code-block:: console

    $ git fetch upstream
    $ git merge upstream/main

Then, download the latest validated data from AWS S3 storage buckets into your
local PUDL output folder with:

.. code-block:: console

    $ aws s3 cp s3://pudl.catalyst.coop/nightly/pudl.sqlite.zip $PUDL_OUTPUT
    --no-sign-request
    $ unzip $PUDL_OUTPUT/pudl.sqlite.zip -d $PUDL_OUTPUT


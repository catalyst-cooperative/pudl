=======================================================================================
Usage Modes
=======================================================================================

Once we process the raw data through the :doc:`PUDL pipeline<pudl_overview>`, we publish
the PUDL data in various formats and provide users with several means of accessing the
data. We are always trying to increase accessibility of the PUDL data, so if you have
suggestions or questions feel free to :ref:`submit an issue via GitHub<aq_on_gh>` or
send us a message at hello@catalyst.coop.

.. _usage-modes:

---------------------------------------------------------------------------------------
How Should You Access PUDL Data?
---------------------------------------------------------------------------------------

We provide four primary ways of interacting with PUDL data. Here's how to find out
which one is right for you and your use case.

+-----------------+----------------+---------------------------------------------------+
| Usage Tool      | User           | Usage Description                                 |
+=================+================+===================================================+
|:ref:`dtstt`     | Curious        | For users who want to view PUDL data in a web     |
|                 | Explorer /     | browser. For uses who want to determine if PUDL   |
|                 | Spreadsheet    | has the data they need to do their analysis.      |
|                 | User           | Enables users to download PUDL tables as CSVs to  |
|                 |                | do analysis in spreadsheets.                      |
+-----------------+----------------+---------------------------------------------------+
| :ref:`zen-doc`  | Jupyter        | For users who want a stable, citable version of   |
|                 | Notebook User /| the fully processed PUDL data on your computer.   |
|                 | Database User  | Users can interact with the PUDL data directly    |
|                 |                | or use the PUDL tools that help users interact    |
|                 |                | with PUDL data. Using PUDL tools requires         |
|                 |                | setting up Docker, but this will enable platform  |
|                 |                | independent functionality of PUDL tools.          |
+-----------------+----------------+---------------------------------------------------+
| :ref:`jup-hub`  | Jupyter        | For users who want to play with PUDL data and     |
|                 | Notebook User /| run through the PUDL tutorial notebooks with      |
|                 | New Python User| minimal setup but minimal computational           |
|                 |                | resources. For users who are interested in using  |
|                 |                | PUDL data in a JupyterLab notebook, but don’t     |
|                 |                | want the data on their computer.                  |
+-----------------+----------------+---------------------------------------------------+
| :ref:`dev-env`  | Experienced    | For users who want to work directly with the      |
|                 | Python User /  | PUDL software to run the data processing.         |
|                 | Open Source    | pipeline. For users who want to tweak the         |
|                 | Contributor    | standard PUDL ETL or contribute to the codebase.  |
+-----------------+----------------+---------------------------------------------------+

---------------------------------------------------------------------------------------
Usage Instructions
---------------------------------------------------------------------------------------

.. _dtstt:

Datasette
^^^^^^^^^

For users seeking quick, visual access to PUDL data, we have set up an experimental
version of a web interface for PUDL tables via `Datasette <https://docs.datasette.io/en/stable/>`_,
an open source tool that helps make data more interactive. Through Datasette, users can
browse PUDL tables, select portions of them using dropdown menus, write SQL queries, and
download PUDL data to CSVs. Visit our Datasette instance at
`<https://data.catalyst.coop>`_ to begin exploring the tables. This is a work in
progress and currently provides access to a limited amount of normalized PUDL tables.

.. _zen-doc:

Zenodo Archives & Docker
^^^^^^^^^^^^^^^^^^^^^^^^

Catalyst periodically publishes our fully processed data as SQLite tables and parquet
files alongside a PUDL Docker image in a data archive on `Zenodo
<https://zenodo.org/communities/catalyst-cooperative/?page=1&size=20>`_ . A Docker image
functions as a set of instructions that create a "container" capable of executing a
specified application on the Docker platform. This is useful when an application
requires substantial setup or processing prior to use. The PUDL Docker image outlines
the steps necessary to run the ETL so that users don't have to do it themselves every
time they want to access the processed data or use PUDL functions.  Users can choose to
interact directly with the processed PUDL data, through SQL or the parquet files, or run
the Docker image to use the data within a Docker container. Using Docker provides the
user with access to all of the denormalized output tables and analysis functions
available through the PUDL software and is recommended for users seeking greater
analysis capabilities.

To begin using Docker, you'll need to
`install Docker & docker-compose <https://docs.docker.com/get-docker/>`_,
download this big compressed tarball, extract the tarball in a directory on your
computer, and run docker image load.

.. code-block:: console

   $ EXAMPLE
   $ docker image load --input filename.tar


Make sure you don't have another Jupyter Notebook server running on port 8888 already by
running the foreground commend ``fg``. If you do, press ``ctr c`` to quit. Continue
testing the ``fg`` command until it outputs ``no such job``.

.. code-block:: console

   $ fg

Run docker-compose up (need to refer to the just-loaded image, not the docker hub
reference) Visit the URL it prints out which should start with:
``https://127.0.0.1:8888`` or ``https://localhost:8888``

.. code-block:: console

    $ docker-compose up

Opening the docker container will open PUDL’s JupyterLab example notebooks:

* `Accessing PUDL SQLite tables <https://github.com/catalyst-cooperative/pudl-tutorials/blob/main/notebooks/01-pudl-database.ipynb>`_
* `Accessing PUDL’s de-normalized and analysis tables <https://github.com/catalyst-cooperative/pudl-tutorials/blob/main/notebooks/02-pudl-outputs.ipynb>`_
* `Accessing the larger-than-memory PUDL data stored in parquet files <https://github.com/catalyst-cooperative/pudl-tutorials/blob/main/notebooks/03-pudl-parquet.ipynb>`_

.. _jup-hub:

JupyterHub
^^^^^^^^^^

PUDL’s `JupyterHub <https://jupyter.org/hub>`_ is currently experimental. It has access
to all of PUDL’s fully processed data, the PUDL example notebooks enumerated in the Data
Release section above and computational resources.

This setup is similar to using the Zenodo archives with the Docker container, but it
avoids downloading the pre-processed data and navigating Docker. We plan to updating the
data we are providing access to via JupyterHub more often than we will publish new data
archives on Zenodo.

To access the JupyterHub, you'll need to:

* Submit a `request form <https://docs.google.com/forms/d/1i8O5kbExFkR-urrmYPdWSIDl-Aub-nx5eMFgOn6TTnM/edit>`_
  to be added to our JupyterHub.
* Once you get your credentials, login
* And…

.. _dev-env:

Development Environment
^^^^^^^^^^^^^^^^^^^^^^^

This use mode requires both setting up the computational environment and running the
PUDL processing pipeline. See the detailed
:doc:`development setup page <dev/dev_setup>`.

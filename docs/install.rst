Installation and Setup
=======================

.. _install-requirements:

-------------------------------------------------------------------------------
System Requirements
-------------------------------------------------------------------------------

.. note::

    The PUDL data processing pipeline does a lot of work in-memory with
    :class:`pandas.DataFrame` objects. Exhaustive record linkage within the
    25 years of :ref:`data-ferc1` data requires up to **24 GB** of memory.
    The full :ref:`data-epacems` dataset is nearly **100 GB** on disk
    uncompressed.

Python 3.7+ (and conda)
^^^^^^^^^^^^^^^^^^^^^^^

PUDL requires Python 3.7 (but is not quite yet working on Python 3.8). While
not strictly necessary, we **highly** recommend using the most recent version
of `Anaconda Python <https://www.anaconda.com/distribution/>`__, or its smaller
cousin `miniconda <https://conda.io/miniconda.html>`__ if you are fond of the
command line and want a lightweight install.

Both Anaconda and miniconda provide ``conda``, a command-line tool that helps
you manage your Python software environment, packages, and their dependencies.
PUDL provides an ``environment.yml`` file defining a software environment that
should work well for most users in conjunction with ``conda``.

We recommend using ``conda`` because while PUDL is written entirely in Python,
it makes heavy use of Python's open data science stack including packages like
:mod:`numpy`, :mod:`scipy`, :mod:`pandas`, and :mod:`sklearn` which depend on
extensions written in C and C++. These extensions can be difficult to build
locally when installed with ``pip``, but ``conda`` provides pre-compiled
platform specific binaries that should Just Work™.

.. _install-pudl:

-------------------------------------------------------------------------------
Installing the Package
-------------------------------------------------------------------------------

PUDL and all of its dependencies are available via ``conda`` on the community
manged `conda-forge <https://conda-forge.org/>`__ channel, and we recommend
installing PUDL within its own ``conda`` environment like this:

.. code-block:: console

    $ conda create --yes --name pudl --channel conda-forge \
        --strict-channel-priority python=3.7 catalystcoop.pudl pip

Then you activate that ``conda`` environment to access it:

.. code-block:: console

    $ conda activate pudl

Once you've activated the pudl environment, you may want to install additional
software within it, for example if you want to use Jupyter notebooks to work
with PUDL interactively:

.. code-block:: console

    $ conda install jupyter jupyterlab

You may also want to update your global ``conda`` settings:

.. code-block:: console

    $ conda config --add channels conda-forge
    $ conda config --set channel_priority strict

PUDL is also available via the official
`Python Package Index <https://pypi.org>`_ (PyPI) and be installed with
``pip`` like this:

.. code-block:: console

    $ pip install catalystcoop.pudl

.. note::

    ``pip`` will only install the dependencies required for PUDL to work as a
    development library and command line tool. If you want to check out the
    source code from Github for development purposes, see the
    :doc:`dev_setup` documentation.

In addition to making the :mod:`pudl` package available for import in Python,
installing ``catalystcoop.pudl`` provides the following command line tools:

* ``pudl_setup``
* ``pudl_datastore``
* ``ferc1_to_sqlite``
* ``pudl_etl``
* ``datapkg_to_sqlite``
* ``epacems_to_parquet``

For information on how to use these scripts, each can be run with the
``--help`` option. ``ferc1_to_sqlite`` and ``pudl_etl`` are configured with
YAML files. Examples are provided with the ``catalystcoop.pudl`` package, and
deployed by running ``pudl_setup`` as described below. Additional information
about the settings files can be found in our documentation on
:ref:`settings_files`

.. _install-workspace:

-------------------------------------------------------------------------------
Creating a Workspace
-------------------------------------------------------------------------------

PUDL needs to know where to store its big piles of inputs and outputs. It
also provides some example configuration files and
`Jupyter <https://jupyter.org>`__ notebooks. The ``pudl_setup`` script lets
PUDL know where all this stuff should go. We call this a "PUDL workspace":

.. code-block:: console

    $ pudl_setup <PUDL_DIR>

Here <PUDL_DIR> is the path to the directory where you want PUDL to do its
business -- this is where the datastore will be located, and where any outputs
that are generated end up. The script will also put a configuration file in
your home directory, called ``.pudl.yml`` which records the location of this
workspace and uses it by default in the future. If you run ``pudl_setup`` with
no arguments, it assumes you want to use the current working directory.

The workspace is laid out like this:

==================== ==========================================================
**Directory / File** **Contents**
-------------------- ----------------------------------------------------------
``data/``            Raw data, automatically organized by source, year, etc.
-------------------- ----------------------------------------------------------
``datapkg/``         `Tabular data packages <https://frictionlessdata.io/specs/tabular-data-package/>`__ generated by PUDL.
-------------------- ----------------------------------------------------------
``environment.yml``  A file describing the PUDL
                     `conda environment <https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html>`__.
-------------------- ----------------------------------------------------------
``notebook/``        Interactive `Jupyter <https://jupyter.org>`__
                     notebooks that use PUDL.
-------------------- ----------------------------------------------------------
``parquet/``         `Apache Parquet <https://parquet.apache.org/>`__ files
                     generated by PUDL.
-------------------- ----------------------------------------------------------
``settings/``        Example configuration files for controlling PUDL scripts.
-------------------- ----------------------------------------------------------
``sqlite/``          :mod:`sqlite3` databases generated by PUDL.
==================== ==========================================================

.. _install-conda-env:

-------------------------------------------------------------------------------
The PUDL ``conda`` Environment
-------------------------------------------------------------------------------
In addition to creating a ``conda`` environment using the command line
arguments referred to above you can specify an environment in a file, usually
named ``environment.yml``. We deploy a basic version of this file into a
PUDL workspace when it's created, as listed above.

Create the Environment
^^^^^^^^^^^^^^^^^^^^^^
Because you won't have the ``environment.yml`` file until after you've
installed PUDL, you will probably create your PUDL environment on the command
line as described above. To do the same thing using an environment file, you'd
run:

.. code-block:: console

   $ conda env create --name pudl --file environment.yml

You may want to periodically update PUDL and the packages it depends on
by running the following commands in the directory with ``environment.yml``
in it:

.. code-block:: console

    $ conda update conda
    $ conda env update pudl

If you get an error ``No such file or directory: environment.yml``, it
probably means you aren't in the same directory as the ``environment.yml``
file.


Activate the Environment
^^^^^^^^^^^^^^^^^^^^^^^^
``conda`` allows you to set up different software environments for different
projects. However, this means you need to tell ``conda`` which environment you
want to be using at any given time. To select a particular ``conda``
environment (like the one named ``pudl`` that you just created) use ``conda
activate`` followed by the name of the environment you want to use:

.. code-block:: console

   $ conda activate pudl

After running this command you should see an indicator (like ``(pudl)``) in
your command prompt, signaling that the environment is in use.

.. seealso::

    `Managing Environments <https://conda.io/docs/user-guide/tasks/manage-environments.html>`__, in the ``conda`` documentation.

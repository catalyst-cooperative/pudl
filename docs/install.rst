Installation and Setup
=======================

System Requirements
-------------------

Hardware
^^^^^^^^

The ETL pipeline does a lot of processing with :class:`pandas.DataFrame` in
memory, and the full :ref:`data-epacems` dataset is nearly 100 GB uncompressed.
To handle all of the data that is available via PUDL your system would need:

* **8 GB of memory**
* **100 GB of free disk space**

Python 3.7+
^^^^^^^^^^^

PUDL requires Python 3.7 or later. While not strictly necessary, we **highly**
recommend using the most recent version of the
`Anaconda Python distribution <https://www.anaconda.com/distribution/>`__, or
its smaller cousin `miniconda <https://conda.io/miniconda.html>`__
(if you are fond of the command line and want a lightweight install).

Both Anaconda and miniconda provide ``conda``, a command-line tool that helps
you manage your Python software environment, packages, and their dependencies.
PUDL provides an ``environment.yml`` file defining a software environment that
should work well for most users.

.. _install-pip:

Installing PUDL
----------------

PUDL is available via the `Python Package Index <https://pypi.org>`_ (PyPI) and
be installed with ``pip``:

.. code-block:: console

    $ pip install catalyst.pudl

.. note::

    If you are interested in contributing to the PUDL project, and want to
    check out the source code for development purposes, see the section on
    :doc:`contributing <CONTRIBUTING>`.

.. todo::

    Fill out the precise details of installation after we've tested it with a
    pre-release.

.. _install-workspace:

Creating a PUDL Workspace
--------------------------

PUDL manages a bunch of input and output data, and needs to know where to store
it. In addition the package provides some example configuration files and
`Jupyter notebooks <https://jupyter.org>`__. To tell it where to put this stuff
run the ``pudl_setup`` script:

.. code-block:: console

    $ pudl_setup <PUDL_DIR>

where <PUDL_DIR> is the path to the directory in which you want PUDL to do its
business -- this is where the datastore will be located, and any outputs that
are generated will end up. This command will also put a configuration file in
your home directory, called ``.pudl.yml`` that records the location of this
workspace for future reference.

The workspace is laid out like this:

* data
* settings
* notebooks
* datapackage
* sqlite

.. todo::

    Flesh out description of PUDL workspace layout/contents

.. _install-conda-env:

The PUDL conda Environment
--------------------------

.. todo::

    Describe creation and activation of PUDL conda environment.

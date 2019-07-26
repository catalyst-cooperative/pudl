===============================================================================
Development Setup
===============================================================================

If you want to contribute code or documentation, you'll need to create your own
fork of the project on Github, set up the :doc:`build and testing <testing>`
environment on your local system, and make pull requests. If you're new to git
and Github, you may want to check out `this article on collaborative
development models
<https://help.github.com/en/articles/about-collaborative-development-models>`_
and `this one on the Github workflow
<https://guides.github.com/introduction/flow/>`_

The setup process ought to look something like this...

  * Ensure you've got Python 3.7 installed, preferably via ``conda``
  * Fork the PUDL repository to your own Github account.
  * Clone your fork of PUDL to your local computer.
  * Create the PUDL ``conda`` environment
  * Use ``pip`` to install the pudl package in an editable form.
  * Run the PUDL setup script to create a data management environment.
  * Try running the data processing pipeline make sure everything is working.
  * Download whatever data you intend to work with into the PUDL datastore.
  * Run the full data processing pipeline with all the data you downloaded.
  * Run the packaging, ETL, data validation, and documentation tests.
  * Your Python programming tools of choice to edit, test, and run code.

.. todo::

    Check that the following reflects the exact details of a real setup.

.. code-block:: console

    $ git clone https://github.com/catalyst-cooperative/pudl.git
    $ cd pudl
    $ conda env create --name pudl --file environment.yml
    $ pip install -e ./
    $ pudl_setup --pudl_dir=../pudl_workspace
    $ pudl_etl ../pudl_workspace/settings/pudl_etl_example.yml
    $ pudl_datastore --sources eia923,eia860,ferc1,epacems,epaipm
    $ tox -e etl -- --fast --pudl_in=AUTO --pudl_out=AUTO


Clone the PUDL Repository
^^^^^^^^^^^^^^^^^^^^^^^^^

If you aren't familiar with git and Github yet, check out this guide to
`Cloning a Repository
<https://help.github.com/articles/cloning-a-repository/>`__.

Depending on your platform (Linux, OSX, Windows...) and git client you're using
to access GitHub, the exact cloning process might be different, but if you're
using a UNIX-like terminal, the command will look like this:

.. code-block:: console

   $ git clone https://github.com/catalyst-cooperative/pudl.git

This will download the whole history of the project, including the most recent
version, and put it in a directory called ``pudl``.

Create a PUDL conda environment
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To make sure that all of the other software that PUDL depends on is available,
we use a ``conda`` environment, which is described by the ``environment.yml``
file in the repository. You can obtain ``conda`` by installing either the
`Anaconda Python distribution <https://www.anaconda.com/distribution/>`__, or
its lightweight command-line sibling, `miniconda
<https://docs.conda.io/en/latest/miniconda.html>`__

To create the ``conda`` environment on your own machine, from within that top
level ``pudl`` directory in a terminal window type:

.. code-block:: console

   $ conda env create --name=pudl --file=environment.yml

This will probably download a bunch of Python packages, and might take a while.
Future updates to the ``conda`` environment will be much faster, since only a
couple of packages typically get updated at a time.

If you get an error ``No such file or directory: environment.yml``\ , it
probably means you aren't in the ``pudl`` repository directory.
See the ``conda`` documentation for more on `managing environments
<https://conda.io/docs/user-guide/tasks/manage-environments.html>`__.

Activate the PUDL environment
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The beauty of software environments like the ones that ``conda`` helps manage
is that you can have lots of them, tailored to different work. However, this
means you need to tell the system which one you want to be using at any given
time. To select a particular ``conda`` environment (like the one named ``pudl``
that you just created) from within a UNIX-like shell, you'll use ``conda
activate`` followed by the environment you want to use:

.. code-block:: console

   $ conda activate pudl

After running this command you should see an indicator (like ``(pudl)``) to the
left of your command line prompt, that tells you which environment is in use.

Install the PUDL package for development
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The above commands installed the packages that ``pudl`` depends on, but not
``pudl`` itself. Until we've released the package to PyPI, you need to install
it manually from your clone of the repository. This will allow you to use the
PUDL library as if it were a normal package installed from the Python Package
Index. Make sure you're in the top level directory of the repository, and run:

.. code-block:: sh

   pip install --editable ./

The ``--editable`` option keeps ``pip`` from copying files into to the
``site-packages`` directory, and just creates references directly to the
current directory (aka ``./``\ ).

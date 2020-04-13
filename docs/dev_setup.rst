===============================================================================
Development Setup
===============================================================================

If you want to contribute code or documentation directly, you'll need to create
your own fork of the project on Github, and set up some version of the
development environment described below, before making pull requests to submit
new code, documentation, or examples of use.

.. note::

    If you're new to git and Github, you may want to check out:

    * `The Github Workflow <https://guides.github.com/introduction/flow/>`__
    * `Collaborative Development Models <https://help.github.com/en/articles/about-collaborative-development-models>`_
    * `Forking a Repository <https://help.github.com/en/articles/fork-a-repo>`__
    * `Cloning a Repository <https://help.github.com/articles/cloning-a-repository/>`__

------------------------------------------------------------------------------
Install Python 3.7
------------------------------------------------------------------------------

As mentioned in the :doc:`install` documentation, PUDL currently requires
Python 3.7. We use
`Anaconda <https://www.anaconda.com/distribution/>`__ or
`miniconda <https://docs.conda.io/en/latest/miniconda.html>`__ to manage our
software environments. While using ``conda`` isn't strictly required, it does
make everything easier to have everyone on the same platform.

------------------------------------------------------------------------------
Fork and Clone the PUDL Repository
------------------------------------------------------------------------------

On the `main page of the PUDL repository <https://github.com/catalyst-cooperative/pudl>`__
you should see a **Fork** button in the upper right hand corner.
`Forking the repository <https://help.github.com/en/articles/fork-a-repo>`__
makes a copy of it in your personal (or organizational) account on Github that
is independent of, but linked to, the original "upstream" project.

Depending on your operating system and the git client you're using to access
Github, the exact cloning process might be different, but if you're using a
UNIX-like terminal, `cloning the repository <https://help.github.com/articles/cloning-a-repository/>`__
from your fork will look like this (with your own Github username or
organizational name in place of ``USERNAME`` of course):

.. code-block:: console

   $ git clone https://github.com/USERNAME/pudl.git

This will download the whole history of the project, including the most recent
version, and put it in a local directory called ``pudl``.

Repository Organization
^^^^^^^^^^^^^^^^^^^^^^^

Inside your newly cloned local repository, you should see the following:

==================== ==========================================================
**Directory / File** **Contents**
-------------------- ----------------------------------------------------------
``devtools/``        Development tools not distributed with the package.
``docs/``            Documentation source files for `Sphinx <https://www.sphinx-doc.org/en/master/>`__ and `Read The Docs <https://readthedocs.io>`__.
``LICENSE.txt``      A copy of the `MIT License <https://opensource.org/licenses/MIT>`__, under which PUDL is distributed.
``MANIFEST.in``      Template describing files included in the python package.
``notebooks/``       Jupyter Notebooks, examples and development in progress.
``pyproject.toml``   Configuration for development tools used with the project.
``README.rst``       Concise, top-level project documentation.
``setup.py``         Python build and packaging script.
``src/``             Package source code, isolated to avoid unintended imports.
``test/``            Modules for use with `PyTest <http://docs.pytest.org/en/latest/>`__.
``tox.ini``          Configuration for the `Tox <https://tox.readthedocs.io/en/latest/>`__ build and test framework.
==================== ==========================================================

-------------------------------------------------------------------------------
Create and activate the pudl-dev conda environment
-------------------------------------------------------------------------------

Inside the ``devtools`` directory of your newly cloned repository, you should
see an ``environment.yml`` file, which specifies the ``pudl-dev`` ``conda``
environment.  You can create that environment locally from within the main
repository directory by running:

.. code-block:: console

    $ conda update conda
    $ conda config --set channel_priority strict
    $ conda env create --name pudl-dev --file devtools/environment.yml
    $ conda activate pudl-dev

This environment mostly includes additional code quality assurance and testing
packages, on top of the basic PUDL requirements.

-------------------------------------------------------------------------------
Install PUDL for development
-------------------------------------------------------------------------------

The ``catalystcoop.pudl`` package isn't part of the ``pudl-dev`` environment
since you're going to be editing it. To install the local version that now
exists in your cloned repository using ``pip``, into your ``pudl-dev``
environment from the main repository directory (containing ``setup.py``) run:

.. code-block:: console

    $ pip install --editable ./

-------------------------------------------------------------------------------
Install PUDL QA/QC tools
-------------------------------------------------------------------------------
We use automated tools to apply uniform coding style and formatting across the
project codebase. This reduces merge conflicts, makes the code easier to read,
and helps catch bugs before they are committed. These tools are part of the
pudl conda environment, and their configuration files are checked into the
Github repository, so they should be installed and ready to go if you've cloned
the pudl repo and are working inside the pudl conda environment.

These tools can be run at three different stages in development:

* inside your `text editor or IDE <https://realpython.com/python-ides-code-editors-guide/>`__,
  while you are writing code or documentation,
* before you make a new commit to the repository using Git's
  `pre-commit hook scripts <https://pre-commit.com/>`__,
* when the :doc:`tests are run <testing>` -- either locally or on a
  `continuous integration (CI) <https://en.wikipedia.org/wiki/Continuous_integration>`__
  platform (PUDL uses
  `Travis CI <https://travis-ci.org/catalyst-cooperative/pudl>`__).

.. seealso::

    `Real Python Code Quality Tools and Best Practices <https://realpython.com/python-code-quality/>`__
    gives a good overview of available linters and static code analysis tools.

flake8
^^^^^^
`Flake8 <http://flake8.pycqa.org/en/latest/>`__ is a popular Python
`linting <https://en.wikipedia.org/wiki/Lint_(software)>`__ framework, with a
large selection of plugins. We use it to run the following checks:

* `PyFlakes <https://github.com/PyCQA/pyflakes>`__, which checks Python code
  for correctness,
* `pycodestyle <http://pycodestyle.pycqa.org/en/latest/>`__ which checks
  whether code complies with :pep:`8` formatting guidelines,
* `mccabe <https://github.com/PyCQA/mccabe>`_ a tool that measures
  `code complexity <https://en.wikipedia.org/wiki/Cyclomatic_complexity>`__
  to highlight functions that need to be simplified or reorganized.
* `pydocstyle <http://www.pydocstyle.org/en/4.0.0/>`__ checks that Python
  docstrings comply with :pep:`257` (via the flake8-docstrings plugin).
* `pep8-naming <https://github.com/PyCQA/pep8-naming>`__ checks that variable
  names comply with Python naming conventions.
* `flake8-builtins <https://github.com/gforcada/flake8-builtins>`__ checks to
  make sure you haven't accidentally clobbered any reserved Python names with
  your own variables.

doc8
^^^^^
`Doc8 <https://github.com/PyCQA/doc8>`__ is a lot like flake8, but for Python
documentation written in the reStructuredText format and built by
`Sphinx <https://www.sphinx-doc.org/en/master/>`__. This is the de-facto
standard for Python documentation. The ``doc8`` tool checks for syntax errors
and other formatting issues in the documentation source files under the
``docs/`` directory.

autopep8
^^^^^^^^
Instead of just alerting you that there's a style issue in your Python code,
`autopep8 <https://github.com/hhatto/autopep8>`__ tries to fix it
automatically, applying consistent formatting rules based on :pep:`8`.

isort
^^^^^^
Similarly `isort <https://isort.readthedocs.io/en/latest/>`__ consistently
groups and orders Python import statements in each module.

Python Editors
^^^^^^^^^^^^^^
Many of the tools outlined above can be run automatically in the background
while you are writing code or documentation, if you are using an editor that
works well with for Python development. A couple of popular options are the
free `Atom editor <https://atom.io/>`__ developed by Github, and the less free
`Sublime Text editor <https://www.sublimetext.com/>`__. Both of them have
many community maintained addons and plugins.

.. seealso::

    `Real Python Guide to Code Editors and IDEs <https://realpython.com/python-ides-code-editors-guide/>`__

Catalyst primarily uses the Atom editor, with the following plugins and
settings. These plugins require that the tools described above are installed
on your system -- which is done automatically in the pudl conda environment.

* `atom-beautify <https://atom.io/packages/atom-beautify>`__
  set to "beautify on save," with ``autopep8`` as the beautifier and formatter,
  and set to "sort imports."
* `linter <https://atom.io/packages/linter>`__ the base linter package used by
  all Atom linters.
* `linter-flake8 <https://atom.io/packages/linter-flake8>`__ set to use
  ``.flake8`` as the project config file.
* `python-autopep8 <https://atom.io/packages/python-autopep8>`__ to actually
  do the work of tidying up.
* `python-indent <https://atom.io/packages/python-indent>`__ to autoindent your
  code as you write, in accordance with :pep:`8`.

Git Pre-commit Hooks
^^^^^^^^^^^^^^^^^^^^
Git hooks let you automatically run scripts at various points as you manage
your source code. "Pre-commit" hook scripts are run when you try to make a new
commit. These scripts can review your code and identify bugs, formatting
errors, bad coding habits, and other issues before the code gets checked in.
This gives you the opportunity to fix those issues first.

Pretty much all you need to do is enable pre-commit hooks:

.. code-block:: console

    $ pre-commit install

The scripts that run are configured in the ``.pre-commit-config.yaml`` file.

In addition to ``autopep8``, ``isort``, ``flake8``, and ``doc8``, the
pre-commit hooks also run
`bandit <https://bandit.readthedocs.io/en/latest/>`__ (a tool for identifying
common security issues in Python code) and several other checks that keep you
from accidentally committing large binary files, leaving
`debugger breakpoints <https://realpython.com/python-debugging-pdb/>`__
in your code, forgetting to resolve merge conflicts, and other gotchas that can
be hard for humans to catch but are easy for a computer.

.. note::

    If you want to make a pull request, it's important that all these checks
    pass -- otherwise :doc:`the build <testing>` will fail, since these same
    checks are tun by the tests on Travis.

.. seealso::

    The `pre-commit project <https://pre-commit.com/>`__: A framework for
    managing and maintaining multi-language pre-commit hooks.


-------------------------------------------------------------------------------
Install and Validate the Data
-------------------------------------------------------------------------------

In order to work on PUDL development, you'll probably need to have a bunch of
the data available locally. Follow the instructions in :ref:`datastore` to set
up a local data management environment and download some data locally, then
:doc:`run the ETL pipeline <usage>` to :doc:`generate some data packages
<datapackages>` and use them to populate a local SQLite database with as much
PUDL data as you can stand (for development, we typically load all of the
available data for ``ferc1``, ``eia923``, ``eia860``, and ``epaipm``, datasets,
but only a single state's worth of data for the much larger ``epacems``
hourly data.)

Using Tox to Validate PUDL
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you've done all of the above, you should be able to use ``tox`` to run our
test suite, and perform data validation.  For example, to validate the data
stored in your PUDL SQLite database, you would simply run:

.. code-block:: console

    $ tox -v -e validate

This process may take 30 minutes to an hour to complete.

-------------------------------------------------------------------------------
Running the Tests
-------------------------------------------------------------------------------

We also use ``tox`` to run PyTest against a packaged and separately installed
version of the local repository package.  Take a peek inside ``tox.ini`` to
see what test environments are available.  To run the same tests that will be
run on Travis CI when you make a pull request, you can run:

.. code-block:: console

    $ tox -v -e travis -- --fast

This will run the linters and pre-commit checks on all the code, make sure that
the docs can be built by Sphinx, and run the ETL process on a single year of
data.  The ``--fast`` is passed through to PyTest by ``tox`` because it is
after the ``--``.  That test will also attempt to download a year of data into
a temporary directory.  If you want to skip the download step and use your
already downloaded datastore, you can point the tests at it with
``--pudl_in=AUTO``:

.. code-block:: console

    $ tox -v -e travis -- --fast --pudl_in=AUTO

Additional details can be found in :ref:`testing`.

-------------------------------------------------------------------------------
Making a Pull Request
-------------------------------------------------------------------------------

Before you make a pull request, please check that:

* Your code passes all of the Travis tests by running them with ``tox``
* You can generate a new complete bundle of data packages, including all the
  available data (with the exception of ``epacems`` -- all the years of a
  couple of states is sufficient for testing.)
* Those data packages can be used to populate an SQLite database locally,
  using the ``datapkg_to_sqlite`` script.
* The ``epacems_to_parquet`` script is able to convert the EPA CEMS Hourly
  Emissions table from the data package into an Apache Parquet dataset.
* The data validation tests can be run against that SQLite database, using
  ``tox -v -e validate`` as outlined above.
* If you've added new data or substantial new code, please also include new
  tests and data validation. See the modules under ``test`` and
  ``test/validate`` for examples.

Then you can push the new code to your fork of the PUDL repository on Github,
and from there, you can make a Pull Request inviting us to review your code and
merge your improvements in with the main repository!


-------------------------------------------------------------------------------
Updating the Development Environment
-------------------------------------------------------------------------------

While working within the development setup, you'll almost certainly need to
pull new changes and update the conda environment. Here are some instructions.
This is basic directory structure that is relevant here: ::

  pudl
    ├── devtools
    |    └── environment.py
    ├── src/pudl
    └── setup.py

From inside the ``pudl`` repository, pull the recent changes:

  .. code-block:: console

     $ git pull

To update the instructions for the pudl-dev conda environment, move to the
devtools directory (where the ``environment.py`` file lives) and update:

 .. code-block:: console

    $ cd devtools
    $ conda env update pudl-dev

Now that your pudl-dev is updated, activate it. All instructions below this
assume you stay inside the pudl-dev enironment.

 .. code-block:: console

    $ conda activate pudl-dev

If any pudl scripts or modules have been added or deleted, you'll need to
reinstall the pudl package. If you don't do this First, you need to move back
to the top level ``pudl`` repository, where the ``setup.py`` module lives.

  .. code-block:: console

      $ .. # move into the top-level pudl directory
      $ pip install --editable ./


If you need to update any of your raw data, data packages or database, now is
the time to do that. See the Basic Usage page for that. Now if you are also
working with a pudl-adjacent repository that relies on ``pudl`` and the
``pudl-dev`` conda environment, you should be setup and ready to go.

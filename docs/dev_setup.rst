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

-------------------------------------------------------------------------------
Overview
-------------------------------------------------------------------------------
The setup process ought to look something like this...

  * Ensure you've got Python 3.7 installed, preferably via ``conda``
  * Fork the PUDL repository to your own Github account.
  * Clone your fork of PUDL to your local computer.
  * Create the PUDL ``conda`` environment
  * Use ``pip`` to install the pudl package in an editable form.
  * Enable git pre-commit hooks.
  * Run the PUDL setup script to create a data management environment.
  * Try running the data processing pipeline make sure everything is working.
  * Download whatever data you intend to work with into the PUDL datastore.
  * Run the full data processing pipeline with all the data you downloaded.
  * Run the packaging, ETL, data validation, and doc tests.
  * Optionally set up your editor / IDE to follow our code style guidelines.

.. todo::

    Check that the following reflects the exact details of a real setup.

.. code-block:: console

    $ git clone https://github.com/USERNAME/pudl.git
    $ cd pudl
    $ conda update conda
    $ conda env create --name pudl --file environment.yml
    $ conda activate pudl
    $ pip install -e ./
    $ cd ..
    $ mkdir pudl_workspace
    $ pudl_setup --pudl_dir=pudl_workspace
    $ cd pudl_workspace
    $ pudl_etl settings/pudl_etl_example.yml
    $ pudl_data --sources eia923,eia860,ferc1,epacems,epaipm
    $ tox -e linters
    $ tox -e docs
    $ tox -e etl -- --fast --pudl_dir=AUTO
    $ pudl_etl settings/pudl_etl_full.yml
    $ tox -e validate --pudl_dir=AUTO
    $ pre-commit install

-------------------------------------------------------------------------------
Fork and Clone the Repository
-------------------------------------------------------------------------------
On the `main page of the PUDL repository <https://github.com/catalyst-cooperative/pudl>`__ you should see a **Fork** button in the upper right hand corner.
`Forking the repository <https://help.github.com/en/articles/fork-a-repo>`__
makes a copy of it in your personal (or organizational) account on Github that
is independent of, but linked to, the original "upstream" project.

Depending on your operating system and the git client you're using to access
Github, the exact cloning process might be different, but if you're using a
UNIX-like terminal, `cloning the repository <https://help.github.com/articles/cloning-a-repository/>`__ from your fork will look like this (with your own
Github username or organizational name in place of ``USERNAME`` of course):

.. code-block:: console

   $ git clone https://github.com/USERNAME/pudl.git

This will download the whole history of the project, including the most recent
version, and put it in a local directory called ``pudl``.

Repository Organization
^^^^^^^^^^^^^^^^^^^^^^^
Inside your newly cloned local repository, you should see the following:

==================== ==========================================================
**Directory / File** **Contents**
``docs/``            Documentation source files for `Sphinx <https://www.sphinx-doc.org/en/master/>`__ and `Read The Docs <https://readthedocs.io>`__.
``src/``             Package source code, isolated to avoid unintended imports.
``results/``         A graveyard of old Jupyter Notebooks and outputs. Ignore!
``scripts/``         Development scripts not distributed with the package.
``test/``            Modules for use with `PyTest <http://docs.pytest.org/en/latest/>`__.
``environment.yml``  File defining the ``pudl`` ``conda`` environment.
``MANIFEST.in``      Template describing files included in the python package.
``pyproject.toml``   Configuration for development tools used with the project.
``setup.py``         Python build and packaging script.
``tox.ini``          Configuration for the `Tox <https://tox.readthedocs.io/en/latest/>`__ build and test framework.
==================== ==========================================================

.. todo::

    Delete ``ci`` directory when postgres is deprecated.

-------------------------------------------------------------------------------
Development Environment
-------------------------------------------------------------------------------

Install PUDL for Development
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Cloning the PUDL repository to your computer allows you to edit the code, but
you also need to *install* that code for use, if you want to be able to see and
experiment with the effects of your edits. To edit and use the same code, a
Python package needs to be installed in "editable" (aka "development") mode.

From within the top level of the cloned repository (the directory which
contains ``setup.py``), run:

.. code-block:: console

   $ pip install --editable ./

The ``--editable`` option keeps ``pip`` from copying files into to the
``site-packages`` directory, and instead creates references to the code you'll
be editing, which is inside the the current current directory (also known as
``./``).

Automated Code Checking
^^^^^^^^^^^^^^^^^^^^^^^
We use automated tools to apply uniform coding style and formatting across the
project codebase. This reduces merge conflicts, makes the code easier to read,
and helps catch bugs before they are committed. These tools are part of the
pudl conda environment, and their configuration files are checked into the
Github repository, so they should be installed and ready to go if you've cloned
the pudl repo and are working inside the pudl conda environment.

These tools can be run at three different stages in development:

* inside your `text editor or IDE <https://realpython.com/python-ides-code-editors-guide/>`__, while you are writing code or documentation,
* before you make a new commit to the repository using Git's
  `pre-commit hook scripts <https://pre-commit.com/>`__,
* when the :doc:`tests are run <testing>` -- either locally or on a
  `continuous integration (CI) <https://en.wikipedia.org/wiki/Continuous_integration>`__ platform
  (PUDL uses `Travis CI <https://travis-ci.org/catalyst-cooperative/pudl>`__).

.. seealso::

    `Real Python Code Quality Tools and Best Practices <https://realpython.com/python-code-quality/>`__ gives a good overview of available linters and
    static code analysis tools.

flake8
~~~~~~
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
~~~~
`Doc8 <https://github.com/PyCQA/doc8>`__ is a lot like flake8, but for Python
documentation written in the reStructuredText format and built by
`Sphinx <https://www.sphinx-doc.org/en/master/>`__. This is the de-facto
standard for Python documentation. The doc8 tool checks for syntax errors and
other formatting issues in the documentation source files under the ``docs/``
directory.

autopep8
~~~~~~~~
Instead of just alerting you that there's a style issue in your Python code,
`autopep8 <https://github.com/hhatto/autopep8>`__ tries to fix it
automatically, applying consistent formatting rules based on :pep:`8`.

isort
~~~~~
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

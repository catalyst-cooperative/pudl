===============================================================================
Development Setup
===============================================================================

This page will walk you through what you need to do if you want to be able to
contribute code or documentation to the PUDL project. It assumes that you're
already familiar with using ``git`` and `GitHub <https://github.com>`__, and
working with the command line.

.. note::

    If you're new to ``git`` and Github, you'll want to check out:

    * `The Github Workflow <https://guides.github.com/introduction/flow/>`__
    * `Collaborative Development Models <https://help.github.com/en/articles/about-collaborative-development-models>`_
    * `Forking a Repository <https://help.github.com/en/articles/fork-a-repo>`__
    * `Cloning a Repository <https://help.github.com/articles/cloning-a-repository/>`__

------------------------------------------------------------------------------
Install conda
------------------------------------------------------------------------------
We use the ``conda`` package manager to specify and update our development
environment, preferentially installing packages from the community maintained
`conda-forge <https://conda-forge.org>`__ distribution channel. We recommend
using `miniconda <https://docs.conda.io/en/latest/miniconda.html>`__ rather
than the large pre-defined collection of scientific packages bundled together
in the Anaconda Python distribution. (You may also want to consider using
`mamba <https://github.com/mamba-org/mamba>`__ -- a drop in replacement for
``conda`` written in C++.)

Once you've installed the  ``miniconda`` (and optionally ``mamba``) package manager,
make sure they are configured to use
`strict channel priority <https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-channels.html#>`__
with the following commands:

.. code-block:: console

    $ conda update conda
    $ conda config --set channel_priority strict

------------------------------------------------------------------------------
Fork and Clone the PUDL Repository
------------------------------------------------------------------------------
Unless you're part of the Catalyst Cooperative organization already, you'll need to fork
`the PUDL repository <https://github.com/catalyst-cooperative/pudl>`__
(see `Forking a repository <https://help.github.com/en/articles/fork-a-repo>`__)
This makes a copy of it in your personal (or organizational) account on GitHub that
is independent of, but linked to, the original "upstream" project.

Then, `clone the repository <https://help.github.com/articles/cloning-a-repository/>`__
from your fork to your local computer where you'll be editing the code or docs.
This will download the whole history of the project, including the most recent
version, and put it in a local directory where you can make changes.

-------------------------------------------------------------------------------
Create the PUDL Development Environment
-------------------------------------------------------------------------------
Inside the ``devtools`` directory of your newly cloned repository, you'll see
an ``environment.yml`` file, which specifies the ``pudl-dev`` ``conda``
environment. You can create and activate that environment from within the
main repository directory by running:

.. code-block:: console

    $ conda update conda
    $ conda env create --name pudl-dev --file devtools/environment.yml
    $ conda activate pudl-dev

This environment installs the ``catalystcoop.pudl`` package directly using
the code in your cloned repository so that it can be edited during
development. It also installs all of the software PUDL depends on, some
packages for testing and quality control, working with interactive Jupyter
Notebooks, and a few Python packages that have binary dependencies which can
be easier to satisfy through ``conda`` packages.

-------------------------------------------------------------------------------
Update the PUDL Development Environment
-------------------------------------------------------------------------------
Periodically you will need to update your development (``pudl-dev``) conda
environment. This will get you newer versions of existing dependencies, and
also incorporate any changes to the environment specification that have been
made by other contributors. The most reliable way to do this is to remove the
existing environment and recreate it.

.. note::

    Different development branches within the repository may specify their own
    slightly different versions of the ``pudl-dev`` conda environment. As a
    result you may need to update your environment when switching from one
    branch to another.


If you want to work with the most recent version of the code on a branch
named ``new-feature``, then from within the top directory of the PUDL
repository you would do:

.. code-block:: console

    $ git checkout new-feature
    $ git pull
    $ conda deactivate
    $ conda update conda
    $ conda env remove --name pudl-dev
    $ conda env create --name pudl-dev --file devtools/environment.yml
    $ conda activate pudl-dev

If you find yourself recreating the environment frequently, and are
frustrated by how long it takes ``conda`` to solve the dependencies, we
recommend using the `mamba <https://github.com/mamba-org/mamba>`__ solver.
You'll want to install it in your ``base`` conda environment -- i.e. with no
conda environment activated):

.. code-block:: console

    $ conda deactivate
    $ conda install mamba

Then the above development environment update process would become:

.. code-block:: console

    $ git checkout new-feature
    $ git pull
    $ conda deactivate
    $ mamba update mamba
    $ mamba env remove --name pudl-dev
    $ mamba env create --name pudl-dev --file devtools/environment.yml
    $ conda activate pudl-dev

If you are working with locally processed data and there have been changes to
the expectations about that data in the PUDL software, you may also need to
regenerate your PUDL SQLite database or other outputs. See :ref:`basic-usage`
for more details.

.. _linting:

-------------------------------------------------------------------------------
Set Up Code Linting
-------------------------------------------------------------------------------
We use several automated tools to apply uniform coding style and formatting
across the project codebase. This is known as
`code linting <https://en.wikipedia.org/wiki/Lint_(software)>`__ and it reduces
merge conflicts, makes the code easier to read, and helps catch some types of
bugs before they are committed. These tools are part of the ``pudl-dev`` conda
environment, and their configuration files are checked into the GitHub
repository, so they should be installed and ready to go if you've cloned the
pudl repo and are working inside the pudl conda environment.

Git Pre-commit Hooks
^^^^^^^^^^^^^^^^^^^^
Git hooks let you automatically run scripts at various points as you manage
your source code. "Pre-commit" hook scripts are run when you try to make a new
commit. These scripts can review your code and identify bugs, formatting
errors, bad coding habits, and other issues before the code gets checked in.
This gives you the opportunity to fix those issues before publishing them.

To make sure they are run before you commit any code, you need to enable the
`pre-commit hooks scripts <https://pre-commit.com/>`__ with this command:

.. code-block:: console

    $ pre-commit install

The scripts that run are configured in the ``.pre-commit-config.yaml`` file.

.. seealso::

    The `pre-commit project <https://pre-commit.com/>`__: A framework for
    managing and maintaining multi-language pre-commit hooks.


.. seealso::

    `Real Python Code Quality Tools and Best Practices <https://realpython.com/python-code-quality/>`__
    gives a good overview of available linters and static code analysis tools.

Code and Docs Linters
^^^^^^^^^^^^^^^^^^^^^
`Flake8 <http://flake8.pycqa.org/en/latest/>`__ is a popular Python
`linting <https://en.wikipedia.org/wiki/Lint_(software)>`__ framework, with a
large selection of plugins. We use it to check the formatting and syntax of
the code and docstrings embedded within the PUDL packages.
`Doc8 <https://github.com/PyCQA/doc8>`__ is a lot like flake8, but for Python
documentation written in the reStructuredText format and built by
`Sphinx <https://www.sphinx-doc.org/en/master/>`__. This is the de-facto
standard for Python documentation. The ``doc8`` tool checks for syntax errors
and other formatting issues in the documentation source files under the
``docs/`` directory.

Automatic Formatting
^^^^^^^^^^^^^^^^^^^^
Rather than alerting you that there's a style issue in your Python code,
`autopep8 <https://github.com/hhatto/autopep8>`__ tries to fix it for you
automatically, applying consistent formatting rules based on :pep:`8`.
Similarly `isort <https://isort.readthedocs.io/en/latest/>`__ automatically
groups and orders Python import statements in each module to minimize diffs
and merge conflicts.

Linting Within Your Editor
^^^^^^^^^^^^^^^^^^^^^^^^^^
If you are using an editor designed for Python development many of these code linting
and formatting tools can be run automatically in the background while you write code or
documentation. Popular editors that work with the above tools include:

* `Visual Studio Code <https://code.visualstudio.com/>`__, from Microsoft (free)
* `Atom <https://atom.io/>`__ developed by GitHub (free), and
* `Sublime Text <https://www.sublimetext.com/>`__ (paid).

.. seealso::

    `Real Python Guide to Code Editors and IDEs <https://realpython.com/python-ides-code-editors-guide/>`__

Catalyst primarily uses the Atom editor with the following plugins. These plugins
require that the tools described above are installed on your system -- which is done
automatically in the ``pudl-dev`` conda environment.

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

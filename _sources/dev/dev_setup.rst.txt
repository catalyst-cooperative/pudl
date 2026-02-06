.. _dev_setup:

===============================================================================
Development Setup
===============================================================================
This page will walk you through what you need to do if you want to be able to contribute
code or documentation to the PUDL project.

These instructions assume that you are working on a Unix-like operating system (MacOS or
Linux) and are already familiar with ``git``, GitHub, and the Unix shell.

.. warning::

    It's not currently possible to install the development environment on Windows unless
    you use the Windows Subsystem for Linux (WSL).

.. note::

    If you're new to ``git`` and `GitHub <https://github.com>`__ , you'll want to
    check out:

    * `The Github Workflow <https://guides.github.com/introduction/flow/>`__
    * `Collaborative Development Models <https://help.github.com/en/articles/about-collaborative-development-models>`__
    * `Forking a Repository <https://help.github.com/en/articles/fork-a-repo>`__
    * `Cloning a Repository <https://help.github.com/articles/cloning-a-repository/>`__


------------------------------------------------------------------------------
Fork and Clone the PUDL Repository
------------------------------------------------------------------------------
Unless you're part of the Catalyst Cooperative organization already, you'll need to fork
`the PUDL repository <https://github.com/catalyst-cooperative/pudl>`__ This makes a copy
of it in your personal (or organizational) account on GitHub that is independent of, but
linked to, the original "upstream" project.

Then, `clone the repository <https://help.github.com/articles/cloning-a-repository/>`__
from your fork to your local computer where you'll be editing the code or docs.  This
will download the whole history of the project, including the most recent version, and
put it in a local directory where you can make changes.

After cloning, you need to configure git to use the "ours" merge strategy for the
lockfile. Run this command once from within the cloned repository:

.. code-block:: console

    $ git config --local merge.ours.driver true

.. warning::

   **Never manually merge pixi.lock!** The lockfile (``pixi.lock``) is a
   generated file that must be regenerated programmatically. The git configuration
   above will automatically keep your version of the lockfile during merge conflicts,
   but you must then regenerate it to reflect the merged dependencies.

   After merging changes from another branch (e.g. ``main``) that modified
   ``pyproject.toml``:

   1. Complete the merge (the lockfile will keep your version due to the merge strategy)
   2. Regenerate the lockfile to match the merged dependencies:

   .. code-block:: console

      $ pixi update

   3. Commit the regenerated lockfile

   This ensures your lockfile is consistent with the merged dependency specifications.

-------------------------------------------------------------------------------
Create the PUDL Dev Environment
-------------------------------------------------------------------------------
We use `pixi <https://pixi.prefix.dev>`__ to manage our development environment.

Pixi installs packages from `conda-forge <https://conda-forge.org>`__. It provides
reproducible environments through lockfiles that specify particular versions of all
PUDL's direct and indirect software dependencies. The dependencies are defined in
``pyproject.toml``. The lockfile is updated weekly by our ``update-lockfiles.yml``
GitHub Action workflow. Pixi also lets us define common tasks, e.g. for running
our tests. Previously we used a mix of ``mamba``, ``conda-lock``, and ``make`` to
provide this functionality. Pixi is much faster, more flexible, under very active,
development, and lets us use one tool instead of three.

See `the Pixi installation guide <https://pixi.prefix.dev/latest/installation/>`__.

With ``pixi`` installed, to install all of PUDL's dependencies and set up the
development environment, from the root of the PUDL repository run:

.. code-block:: console

    $ pixi install

This will create the environment and install the PUDL package in editable mode. Pixi
automatically runs commands within the environment associated with your current
directory when you prefix them with ``pixi run``, so you don't need to manually activate
environments like you would with conda. If you prefer to enable the environment and
forget about it, you can use ``pixi shell``, but note that you will need to run ``exit``
to get out of that shell when switching environments.

``pixi run`` is also used to run tasks that have been defined in ``pyproject.toml``.
To see all the available tasks defined in the project, run:

.. code-block:: console

    $ pixi task list

When defining a new task, make sure the name doesn't clash with a generic shell command,
otherwise the named task will take precedence and make it impossible to run the shell
command in the pixi environment.

There's additional information about running tests in the :doc:`testing` documentation.

-------------------------------------------------------------------------------
Updating the PUDL Development Environment
-------------------------------------------------------------------------------

You will need to periodically update your development environment to get newer versions
of existing dependencies and incorporate any changes to the environment specification
that have been made by other contributors.

The simplest way to update your environment is to run:

.. code-block:: console

    $ pixi install

This will update your local environment to be in sync with the committed ``pixi.lock``
file.

If you are adding or updating specific dependencies in ``pyproject.toml``, you can
update just that dependency:

.. code-block:: console

    $ pixi add "package-name>=version"  # For new dependencies
    $ pixi update package-name           # To update an existing dependency

Our automated GitHub Action handles weekly updates to all dependencies. You should
not generally need to update all dependencies yourself unless you're working on
dependency management. The command to update everything in ``pixi.lock`` is:

.. code-block:: console

   $ pixi update

You can also see what would be updated without actually changing ``pixi.lock`` with:

.. code-block:: console

   $ pixi update --dry-run

.. note::

    Different development branches within the repository may have different lockfiles.
    When switching branches, run ``pixi install`` to sync your environment with the
    branch's lockfile.

If you want to work with the most recent version of the code on a branch named
``new-feature``, then from within the top directory of the PUDL repository you would do:

.. code-block:: console

    $ git checkout new-feature
    $ git pull
    $ pixi install

If you are working with locally processed data and there have been changes to the
expectations about that data in the PUDL software, you may also need to regenerate your
PUDL SQLite database or other outputs. See :doc:`run_the_etl` for more details.

.. _linting:

-------------------------------------------------------------------------------
Set Up Code Linting
-------------------------------------------------------------------------------
We use several automated tools to apply uniform coding style and formatting across the
project codebase. This is known as `code linting
<https://en.wikipedia.org/wiki/Lint_(software)>`__, and it reduces merge conflicts,
makes the code easier to read, and helps catch some types of bugs before they are
committed. These tools are part of the PUDL pixi environment and their configuration
files are checked into the GitHub repository. If you've cloned the pudl repo and are
working inside the pudl conda environment, they should be installed and ready to go.

Git Pre-commit Hooks
^^^^^^^^^^^^^^^^^^^^
Git hooks let you automatically run scripts at various points as you manage your source
code. "Pre-commit" hook scripts are run when you try to make a new commit. These scripts
can review your code and identify bugs, formatting errors, bad coding habits, and other
issues before the code gets checked in.  This gives you the opportunity to fix those
issues before publishing them.

To make sure they are run before you commit any code, you need to enable the
`pre-commit hooks scripts <https://pre-commit.com/>`__ with this command:

.. code-block:: console

    $ pixi run pre-commit-install

The scripts that run are configured in the ``.pre-commit-config.yaml`` file.

.. seealso::

    * The `pre-commit project <https://pre-commit.com/>`__: A framework for
      managing and maintaining multi-language pre-commit hooks.
    * `Real Python Code Quality Tools and Best Practices <https://realpython.com/python-code-quality/>`__
      gives a good overview of available linters and static code analysis tools.

Linting and Formatting
^^^^^^^^^^^^^^^^^^^^^^

* `ruff <https://docs.astral.sh/ruff/>`__ is a popular, fast Python
  `linting <https://en.wikipedia.org/wiki/Lint_(software)>`__ and autofix framework,
  with a large selection of rules that can be configured (often mirroring plugins
  originally developed for ``flake8``). We use it to check the formatting and syntax of
  the code and to ensure that we're all using modern python syntax, type hinting, etc.
* We also use `ruff to format our code <https://docs.astral.sh/ruff/formatter/>`__. It
  serves as a much faster drop-in replacement for longtime crowd favorite `black
  <https://black.readthedocs.io/en/stable/>`__
* `doc8 <https://github.com/PyCQA/doc8>`__ , lints our documentation files, which are
  written in the reStructuredText format and built by `Sphinx
  <https://www.sphinx-doc.org/en/master/>`__. This is the de-facto standard for Python
  documentation. The ``doc8`` tool checks for syntax errors and other formatting issues
  in the documentation source files under the ``docs/`` directory.
* `typos <https://github.com/crate-ci/typos>`__ checks for typos in the committed files.
  If you get an error, run ``typos path/to/file``. Review all proposed corrections
  manually. If typos marks an issue as having more than
  one possible correction, manually identify and implement the desired fix in the file.
  If anything shouldn't be updated (e.g., PUDL is not a typo!), add it to the
  pyproject.toml file under tool.typos.default.extend-words, using the format
  word="word". Run ``typos path/to/file`` again to verify that the remaining flagged
  typos are as anticipated. Once you're satisfied, run ``typos path/to/directory -w``
  to auto-correct any remaining typos. To ignore typos in a single line (e.g., code
  that corrects a typos), add ``# spellchecker:ignore`` as a comment to the end of the
  line.

Linting Within Your Editor
^^^^^^^^^^^^^^^^^^^^^^^^^^
If you are using an editor designed for Python development many of these code linting
and formatting tools can be run automatically in the background while you write code or
documentation. Popular editors that work with the above tools include:

* `Visual Studio Code <https://code.visualstudio.com/>`__, from Microsoft (free, but...)
* `NeoVim <https://neovim.io/>`__, (free and open source; for diehard Unix lovers)
* `PyCharm <https://www.jetbrains.com/pycharm/>`__ (paid).
* `Sublime Text <https://www.sublimetext.com/>`__ (paid).

Each of these editors have their own collection of plugins and settings for working
with linters, formatters, and other code analysis tools.

.. seealso::

    `Real Python Guide to Code Editors and IDEs <https://realpython.com/python-ides-code-editors-guide/>`__

.. _install-workspace:

-------------------------------------------------------------------------------
Creating a Workspace
-------------------------------------------------------------------------------

PUDL Workspace Setup
^^^^^^^^^^^^^^^^^^^^

PUDL needs to know where to store its big piles of inputs and outputs.  The
``PUDL_OUTPUT`` and ``PUDL_INPUT`` environment variables let PUDL know where all this
stuff should go. We call this a "PUDL workspace".

First, create a directory to store local caches of raw PUDL data. You can put this
anywhere, but we put this in ``~/pudl_input`` in the documentation.  Then create an
environment variable called ``PUDL_INPUT`` to store the path to this new directory and
make sure that it is set whenever you start up a new shell. These shorthand commands
will append a line to the end of your shell initialization file. If you need to change
it later you'll want to edit those files directly. Note that in the commands below you
must replace the example path ``/absolute/path/to/pudl_input`` with the actual path to
the directory you've created.

.. code-block:: console

    $ echo "export PUDL_INPUT=/absolute/path/to/pudl_input" >> ~/.zshrc # if you are using zsh
    $ echo "export PUDL_INPUT=/absolute/path/to/pudl_input" >> ~/.bashrc # if you are using bash
    $ set -Ux PUDL_INPUT /absolute/path/to/pudl_input # if you are using fish shell

The directory stored in ``PUDL_INPUT`` contains versions of PUDL's raw data archives on
Zenodo for each datasource:

.. code-block::

    pudl_input/
    ├── ferc1/
    │   ├── 10.5281-zenodo.5534788/
    │   │   ├── datapackage.json
    │   │   ├── ferc1-1994.zip
    │   │   ├── ferc1-1995.zip
    │   │   └── ...
    │   ├── 10.5281-zenodo.7314437/
    │   │   └── ...
    │   └── ...
    ├── eia860/
    │   └── ...
    └── ...

.. warning::

    The data stored at the ``PUDL_INPUT`` directory can grow to be dozens of gigabytes
    in size. This is because when the raw data are updated, a new version of the archive
    is downloaded to the ``PUDL_INPUT`` directory. To slim down the size you can always
    delete out of date archives the code no longer depends on.

Next, create a directory to store the outputs of the PUDL ETL. As above, you can put
this anywhere, but typically this is ``~/pudl_output``. Then, as with ``PUDL_INPUT``,
create an environment variable called ``PUDL_OUTPUT`` to store the path to this new
directory. In the commands below you must replace the example path
``/absolute/path/to/pudl_output`` with the actual path to the directory you want to use.

.. code-block:: console

    $ echo "export PUDL_OUTPUT=/absolute/path/to/pudl_output" >> ~/.zshrc # zsh
    $ echo "export PUDL_OUTPUT=/absolute/path/to/pudl_output" >> ~/.bashrc # bash
    $ set -Ux PUDL_OUTPUT /absolute/path/to/pudl_output # fish

The path stored in ``PUDL_OUTPUT`` contains all ETL outputs like ``pudl.sqlite`` and
``core_epacems__hourly_emissions.parquet``.

.. warning::

    Make sure you set these environment variables to point at separate directories!  It
    is also **strongly recommended** that you create these directories outside of the
    pudl repository directory so the inputs and outputs are not tracked in git.

Remember that you'll need to either source your shell profile after adding the new
environment variable definitions above, or export them at the command line for them to
be active in the current shell. Again, note that these are fake example paths:

.. code-block:: console

    $ export PUDL_OUTPUT=/absolute/path/to/pudl_output
    $ export PUDL_INPUT=/absolute/path/to/pudl_input

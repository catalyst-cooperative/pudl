.. _testing:

===============================================================================
Building and Testing PUDL
===============================================================================

The PUDL Project uses `PyTest <https://pytest.org>`__ to test our code, and
`Tox <https://tox.readthedocs.io>`__ to ensure the tests are run in a
controlled environment. We run the tests locally, and on
`Travis CI <https://travis-ci.org/catalyst-cooperative/pudl/>`__.

-------------------------------------------------------------------------------
Test Data
-------------------------------------------------------------------------------
We use the same testing framework to validate the data products being generated
by PUDL. This makes running the tests a little more complicated than normal. In
addition to specifying what tests should be run, you must specify how much data
should be used, and where that data can be found.

Data Quantity:
^^^^^^^^^^^^^^

* For "fast" tests we use the most recent year of data that's available for all
  data sources, with the exception of :ref:`data-epacems`, for which we only
  do the most recent year of data for a single state.
* For "full" tests we process all of the data that we expect to work, again
  with the exception of :ref:`data-epacems` for which we do only a single state
  (across all years).

Data Source:
^^^^^^^^^^^^

The tests can use data from three different sources, depending on what you're
testing. They can:

* download a fresh copy of the original data,
* use an existing local datastore skipping the download step, or
* use already processed local data, in the case of post-ETL data validation.

Because
`FTP doesn't work on Travis <https://docs.travis-ci.com/user/common-build-problems/#ftpsmtpother-protocol-do-not-work>`__,
and the :ref:`data-ferc1` and :ref:`data-epacems` data can only be downloaded
over FTP, we also keep a small amount of data for those sources in the PUDL
Github repository and use it to populate the datastore for continuous
integration. We download fresh data for the EIA and other data sources that are
available via HTTPS.

-------------------------------------------------------------------------------
Running PyTest
-------------------------------------------------------------------------------
The PyTest suite is organized into two main categories. **ETL** tests and
**data validation** tests.

ETL Tests
^^^^^^^^^

The ETL tests run the data processing pipeline on either the most recent year
of data, or all working years of data. The tests should be marked with
:mod:`pytest.mark` decorators called ``pytest.mark.etl``. Most of the ETL test
functions are stored in the ``test/etl_test.py`` module, but they rely heavily
on fixtures defined in ``test/conftest.py``.  As mentioned above, the data to
be used in the ETL tests can come from several different places. You can also
specify where the data packages output by the tests should be written.

To run the ETL tests using just the most recent year of data (``--fast``) and
download a fresh copy of that data to a temporary location (the default
behavior), you would run:

.. code-block:: console

    $ pytest test/etl_test.py --fast

To use an already downloaded copy of the input data, generated a ferc1
database, in your default :ref:`PUDL workspace <install-workspace>` (which is
specified in ``$HOME/.pudl.yml``), you would run:

.. code-block:: console

    $ pytest test/etl_test.py --fast --pudl_in=AUTO --live_ferc1_db=AUTO

To specify a particular ``pudl_in`` directory, containing a ``data`` directory
and datastore, you would use:

.. code-block:: console

    $ pytest test/etl_test.py --fast --pudl_in=path/to/pudl_in

To change where the output of the ETL pipeline is written, use the
``--pudl_out`` option. By default it will use a temporary directory created by
:mod:`pytest`. As with ``--pudl_in`` you can specify ``AUTO`` if you want the
output to go to your default ``pudl_out`` (as specified in ``$HOME/.pudl.yml``.

.. code-block:: console

    $ pytest test/etl_test.py --fast --pudl_in=AUTO --pudl_out=my/new/outdir

You may also want to consider using ``--disable-warnings`` to avoid seeing a
bunch of clutter from underlying libraries and deprecated uses.

Data Validation Tests
^^^^^^^^^^^^^^^^^^^^^

The data validation tests are organized into datasource specific modules under
``test/validate``. They test the quality and internal consistency of the data
that is output by the PUDL ETL pipeline. Currently they only work on the full
dataset, and do not have a ``--fast`` option. While it is possible to run the
full ETL process and output it in a temporary directory, to then be used by the
data validation tests, that takes a long time, and you don't get to keep the
processed data afterward. Typically we validate outputs that we're hoping to
keep around, so we advise running the data validation on a pre-generated PUDL
SQLite database.

To point the tests at already processed data, use the ``--live_pudl_db`` and
``--live_ferc1_db`` options. The ``--pudl_in`` and ``--pudl_out`` options work
the same as above. E.g.

.. code-block:: console

    $ pytest --live_pudl_db=AUTO --live_ferc1_db=AUTO \
        --pudl_in=AUTO --pudl_out=AUTO test/validate

Data Validation Notebooks
^^^^^^^^^^^^^^^^^^^^^^^^^
We maintain and test a collection of Jupyter Notebooks that use the same
functions as the data validation tests and also produce some visualizations of
the data to make it easier to understand what's wrong when validation fails.
These notebooks are stored in ``test/notebooks`` and they can be validated
with:

.. code-block:: console

    $ pytest --nbval-lax test/notebooks

The notebooks will only run successfully when there's a full PUDL SQLite
database available in your PUDL workspace.

If the data validation tests are failing for some reason, you may want to
launch those notebooks in Jupyter to get a better sense of what's gong on. They
are integrated into the test suite to ensure that they remain functional as the
project evolves.

For the moment, the data validation cases themselves are stored in the
:mod:`pudl.validate` module, but we intend to separate them from the code and
store them in a more compact, programmatically readable format.

-------------------------------------------------------------------------------
Running Tox
-------------------------------------------------------------------------------
`Tox <https://tox.readthedocs.io/en/latest/>`__ is a system for automating
Python packaging and testing processes. When :mod:`pytest` is run as described
above, it has access to the whole PUDL repository (including files that might
not be deployed on a user's system by the packaging script), and it also sees
whatever python packages you happen to have installed in your local environment
(via ``pip`` or ``conda``) which again, may not be anything like what an end
user has on their system when they install :mod:`pudl`.

To ensure that we are testing ``pudl`` as it will be installed for a user who
is using ``pip`` or ``conda``, Tox packages up the code as specified in
``setup.py``, installs it in a virtual environment, and then runs the same
:mod:`pytest` tests, but against *that* version of PUDL, giving us much more
confidence that it will also work if someone else installs it. The behavior of
Tox is controlled by the ``tox.ini`` file in the main repository directory. It
describes several test environments:

* ``linters``: Static code analyses that catch syntax errors and style issues.
* ``etl``: Run the :mod:`pytest` tests in ``test/etl_test.py`` using the
  data specified on the command line (see below).
* ``validate``: Runs the data validation and output tests and validates the
  distributed notebooks. Requires existing PUDL outputs.
* ``docs``: Builds the documentation using
  `Sphinx <https://www.sphinx-doc.org/en/master/>`__ based on the docstrings
  embedded in our code and any additional resources that we have integrated
  under the ``docs`` directory, using the same setup as our documentation on
  `ReadTheDocs <https://readthedocs.org/projects/catalyst-cooperative-pudl/>`__
* ``travis``: Runs the tests included in the ``linters``, ``docs`` and ``etl``
  tests.

.. todo::

    Modify the data validation tests to work on a single year of data, so they
    can be run on Travis and also quickly locally.

Command line arguments like ``--fast`` and ``--pudl_in=AUTO`` will be passed in
to :mod:`pytest` by Tox if you add them after ``--`` on the command line. E.g.
to have Tox run the ETL tests using the most recent year of data, using the
data you already have on hand in your local datastore you would do:

.. code-block:: console

    $ tox -e etl -- --fast --pudl_in=AUTO

There are other test environments defined in ``tox.ini`` -- including one for
each of the individual linters (``flake8``, ``doc8``, ``pre-commit``,
``bandit``, etc.) which are bundled together into the single ``linters`` test
environment for convenience.  There are also ``build`` and ``release`` test
environments that are used to generate and transmit the pudl distribution to
the Python Package Index for publication.

To see what each of these Tox environments is actually doing, you can look at
the ``commands`` section for each of them in ``tox.ini``.

-------------------------------------------------------------------------------
Generating the Documentation
-------------------------------------------------------------------------------
`Sphinx <https://www.sphinx-doc.org/>`__ is a system for
semi-automatically generating Python documentation, based on doc strings and
other content stored in the ``docs`` directory.
`Read The Docs <https://readthedocs.io>`__ is a platform that automatically
re-runs Sphinx for your project every time you make a commit to Github, and
publishes the results online so that you always have up to date docs. It also
archives docs for all of your previous releases so folks using them can see how
things work for their version of the software, even if it's not the most
recent.

Sphinx is tightly integrated with the Python programming language and needs to
be able to import and parse the source code to do its job. Thus, it also needs
to be able to create an appropriate python environment. This process is
controlled by ``docs/conf.py``.

However, the resources available on Read The Docs are not as extensive as on
Travis, and it can't *really* build many of the scientific libraries we depend
on from scratch. Package "mocking" allows us to fake-out the system so that the
imports succeed, even if difficult to compile packages like ``scipy`` aren't
really installed.

If you are editing the documentation, and need to regenerate the outputs as you
go to see your changes reflected locally, from the main directory of the
repository you can run:

.. code-block:: console

    $ sphinx-build -b html docs docs/_build/html

This will only update any files that have been changed since the last time the
documentation was generated. If you need to regenerate all of the documentation
from scratch, then you should remove the existing outputs first:

.. code-block:: console

    $ rm -rf docs/_build
    $ sphinx-build -b html docs docs/_build/html

To run the `doc8 <http://https://github.com/PyCQA/doc8>`__
reStructuredText linter and re-generate the documentation from scratch, you can
use the Tox ``docs`` test environment:

.. code-block:: console

    $ tox -e docs

Note that this will also attempt to regenerate the :mod:`sphinx.autodoc` files
in ``docs/api`` for modules that are meant to be documented, using the
``sphinx-apidoc`` command -- this should catch any new modules or subpackages
that are added to the repository, and may result in new files that need to be
committed to the Github repository in order for them to show up on Read The
Docs.

-------------------------------------------------------------------------------
Python Packaging
-------------------------------------------------------------------------------
In order to distribute a ready-to-use package to others via the Python Package
Index and ``conda-forge`` we need to encapsulate it with some metadata and
enumerate its dependencies. There are several files that guide this process.

``setup.py``
^^^^^^^^^^^^

The ``setup.py`` script in the top level of the repository coordinates the
packaging process, using :mod:`setuptools` which is part of the Python standard
library. ``setup.py`` is really just a single function call, to
:func:`setuptools.setup`, and the parameters of that function are
metadata related to the Python package. Most of them are relatively self
explanatory -- like the name of the package, the license it's being released
under, search keywords, etc. -- but a few are more arcane:

* ``use_scm_version``: Instead of having a hard-coded version that's stored in
  the repository somewhere, handed off to the packaging script, and often out
  of date, pull the version from the source code management (SCM)
  system, in our case git (and Github). To make a release we will first need
  to `tag a particular revision <https://help.github.com/en/articles/creating-releases>`__ in ``git``
  with a version like ``v0.1.0``.

* ``python_requires='>=3.7, <3.8.0a0'``: Specifies the version or versions of
  Python on which the package is expected to run. We require at least Python
  3.7, and as of yet have not gotten everything working on Python 3.8 reliably,
  so we require a version less than Python 3.8.

* ``setup_requires=['setuptools_scm']``: What *other* packages need to be
  installed in order for the packaging script to run? Because we are obtaining
  the package version from our SCM (git/Github) we need the special package
  that lets us do that magic, which is named
  `setuptools_scm <https://github.com/pypa/setuptools_scm>`__. This
  automatically generated version number can then be accessed in the package
  metadata, as is done our top-level ``__init__.py`` file:

  .. code-block:: python

      __version__ = pkg_resources.get_distribution(__name__).version

  This is convoluted, but also a currently accepted best practice. The changes
  to the Python packaging & build system being implemented as a result of
  :pep:`517` and :pep:`518` should improve the situation.

* ``install_requires``: lists all the other packages that need to be installed
  before ``pudl`` can be installed. These are our package dependencies. This
  list plays a role similar to the ``environment.yml`` file in the main
  ``pudl`` repository, but it depends on ``pip`` not ``conda`` -- in the
  packaging system we do not have access to ``conda``. It turns out this makes
  our lives difficult because of the kind of Python packages we depend on. More
  on this below.

* ``extras_require``: a dictionary describing optional packages that can
  be conditionally installed depending on the expected usage of the install.
  For now this is mostly used in conjunction with Tox, to ensure that the
  required documentation and testing packages are installed alongside PUDL in
  the virtual environment.

* ``packages=find_packages('src')``: The ``packages`` parameter takes a list of
  all the python packages to be included in the distribution that is being
  packaged. The :mod:`setuptools.find_packages`  function automatically
  searches whatever directories it is given for any packages and all of their
  subpackages. All of the code we want to distribute to users lives under the
  ``src`` directory.

* ``package_dir={'': 'src'}``: this tells the packaging to treat any modules or
  packages found in the ``src`` directory as part of the ``root`` package of
  the distribution. This is a vestigial parameter that pertains to the
  :mod:`distutils` which are the predecessor to :mod:`setuptools`... but the
  system still depends on them deep down inside. In our case, we don't have any
  modules that aren't part of any package -- everything is within :mod:`pudl`.

* ``include_package_data=True``: This tells the packaging system to include any
  non-python files that it finds in the directories it has been told to
  package. In our case this is all the stuff inside ``package_data`` including
  example settings files, metadata, glue, etc.

* ``entry_points``: This parameter tells the packaging what executable scripts
  should be installed on the user's system, and which modules:functions
  implement those scripts.

``MANIFEST.in``
^^^^^^^^^^^^^^^
In addition to generating a version number automatically based on our git
repository, ``setuptools_scm`` pulls every single file tracked by the
repository and every other random file sitting in the working repository
directory into the distribution. This is... not what we want. ``MANIFEST.in``
allows us to specify in more detail which files should be included and
excluded. Mostly we are just including the python package and supporting data,
which exist under the ``src/pudl`` directory.

``pyproject.toml``
^^^^^^^^^^^^^^^^^^
The adoption of :pep:`517` and :pep:`518` has opened up the possibility of
using build and packaging systems besides :mod:`setuptools`. The new system
uses ``pyproject.toml`` to specify the build system requirements. Other tools
related to the project can also store their settings in this file making it
easier to see how everything is set up, and avoiding the proliferation of
different configuration files for e.g. PyTest, Tox, Flake8, Travis,
ReadTheDocs, bandit...

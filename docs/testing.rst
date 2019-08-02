===============================================================================
Building and Testing PUDL
===============================================================================

-------------------------------------------------------------------------------
Running Tox
-------------------------------------------------------------------------------
`Tox <https://tox.readthedocs.io/en/latest/>`__ is a system for automating
Python packaging and testing processes. The way we have been running
:mod:`pytest` locally, it runs the tests against whatever code happens to be
laying around in the working repository, in whatever Python environment we
happen to be working in. This might not look anything like the code or
environment that ends up getting installed on a user's machine if we have
messed up the packaging, or have installed additional python packages in our
personal workspace without adding them to the package requirements.

To avoid those pitfalls, Tox packages up the code (based on whatever we have
specified in ``setup.py`` and ``MANIFEST.in``, described above), installs it in
a virtual environment, and then runs the tests against *that* version, giving
us much more confidence that it will also work if someone else installs it. The
behavior of Tox is controlled by the ``tox.ini`` file in the main repository
directory.  Currently it defines three "test environments":

* ``etl``\ : Tests datastore creation and the ETL process.
* ``validate``\ : Runs the data validation and output tests and validates the
  distributed notebooks.
* ``docs``\ : Builds the documentation using
  `Sphinx <https://www.sphinx-doc.org/en/master/>`_ based on the docstrings
  embedded in our code and any additional resources that we have integrated
  under the ``docs`` directory, using the same setup as our documentation on
  `ReadTheDocs <https://readthedocs.org/projects/catalyst-cooperative-pudl/>`_

There's a list of python packages required to run the build and packaging for
PUDL in the top level ``testenv`` section of ``tox.ini``\ , and each of the
individual test environments can specify additional dependencies if needed.
Note that this is *yet another place* where package dependencies are being
specified (in addition to ``environment.yml`` and ``setup.py``\ ). This
duplication is bad and will certainly lead to bugs and errors and conflicts
later. Each of the test environments (\ ``etl``\ , ``validate``\ , and
``docs``\ ) also potentially has its own set of different/additional python
dependencies, specified in their respective ``deps`` sections. ``etl`` and
``validate`` use the same dependencies as those listed in the main ``testenv``\
, but the Sphinx/RTD documentation system requires that its dependencies reside
in ``docs/requirements.txt`` which is included by reference in ``tox.ini``
(Yes, a **fourth** specification of package dependencies!!!).

Each of the Tox test environments also includes a ``commands`` section that
specifies what gets run during that process, in addition to the python package
creation and virtual environment installation.

-------------------------------------------------------------------------------
Running PyTest
-------------------------------------------------------------------------------
* Location of the datastore and other inputs.
* Test coverage generation.
* Differences between running the tests locally and on Travis.

-------------------------------------------------------------------------------
Generating the Documentation
-------------------------------------------------------------------------------
Sphinx is a system for semi-automatically generating Python documentation,
based on doc strings and other content stored in the ``docs`` directory. Read
the Docs is a platform that automatically re-runs Sphinx for your project every
time you make a commit to Github, and publishes the results online so that you
always have up to date docs. It also archives docs for all of your previous
releases so folks using them can see how things work for their version of the
software, even if it's not the most recent.

Sphinx doesn't really run the software, but it does need to be able to import
and parse the source code to do its job, so it also needs to be able to create
an appropriate python environment. This process is controlled by
``docs/conf.py``.

However, the resources available on RTD are not as extensive as on Travis, and
it can't *really* build many of the scientific libraries we depend on from
scratch. Package "mocking" allows us to fake-out the system so that the
imports succeed, even if difficult to compile packages like ``scipy`` aren't
really installed.

This is currently not working and I don't know why.

Linters in Tox:

* pre-commit
* doc8
* bandit
* flake8

-------------------------------------------------------------------------------
Python Packaging
-------------------------------------------------------------------------------
In order to distribute a ready-to-use package to others, it must be packaged.

``setup.py``
^^^^^^^^^^^^^^^^

The ``setup.py`` script in the top level of the repository coordinates the
packaging process, using :mod:`setuptools` which is part of the Python standard
library. ``setup.py`` is really just a single function call, to the
:func:`setuptools.setup` function, and the parameters of that function are
metadata related to the Python package. Most of them are relatively self
explanatory -- like the name of the package, the license it's being released
under, search keywords, etc. -- but a few are more arcane. I'll explain the
arcane ones here.

* ``use_scm_version``: Instead of having a hard-coded version that's stored in
  the repository somewhere, handed off to the packaging script, and often ends
  up being out of date, pull the version from the source code management (SCM)
  system, in our case git (and Github). To make a release we will first need
  to `tag a particular
  revision <https://help.github.com/en/articles/creating-releases>`_ in ``git``
  with a version like ``v0.1.0``.

* ``python_requires='>=3.7, <3.8.0a0'``: Specifies the version or versions of
  Python on which the package is expected to run. We require at least Python
  3.7, and it's accepted best practice to preclude packages from getting
  installed on the next major version up, since major versions tend to break
  things. So we require a version less than Python 3.8.

* ``setup_requires=['setuptools_scm']``: What *other* packages need to be
  installed in order for the packaging script to run? Because we are obtaining
  the package version from our SCM (git/Github) we need the special package
  that lets us do that magic, which is named
  `setuptools_scm <https://github.com/pypa/setuptools_scm>`__. This
  automatically generated version number can then be accessed in the package
  metadata, as is done our top-level ``__init__.py`` file:

  .. code-block:: py

      __version__ = pkg_resources.get_distribution(__name__).version

  Yes, this is convoluted. Incredibly, it is also currently the accepted best
  practice.

* ``install_requires``: lists all the other packages that need to be installed
  before ``pudl`` can be installed. These are our package dependencies. This
  list plays a role similar to the ``environment.yml`` file in the main
  ``pudl`` repository, but it depends on ``pip`` not ``conda`` -- in the
  packaging system we do not have access to ``conda``. It turns out this makes
  our lives difficult because of the kind of Python packages we depend on. More
  on this later. To be honest, I'm not 100% sure which packages should be
  included in this list. E.g. should it include ``jupyter`` and
  :mod:`matplotlib` ? As it is now, the ``pudl`` library doesn't really depend
  on them, but we do expect users to use them. However, we're expecting users
  to use ``conda`` to manage their environment, so maybe ``jupyter`` and
  :mod:`matplotlib` should go in ``environment.yml`` and not here?

* ``packages=find_packages('src')``: The ``packages`` parameter takes a list of
  all the python packages to be included in the distribution that is being
  packaged. The `\ ``find_packages()``
  <https://setuptools.readthedocs.io/en/latest/setuptools.html#using-find-packages>`_
  function automatically searches whatever directories it is given for any
  packages and all of their subpackages. All of the code we want to distribute
  to users lives under the ``src`` directory.

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
^^^^^^^^^^^^^^^^^^^
In addition to generating a version number automatically based on our git
repository, ``setuptools_scm`` pulls every single file tracked by the
repository and every other random file sitting in the working repository
directory into the distribution. This is... not what we want. ``MANIFEST.in``
allows us to specify in more detail which files should be included and
excluded. Mostly we are just including the python package and supporting data,
which exist under the ``src/pudl`` directory, as well as the ``test`` modules
and the curated content under ``docs``.

``tox.ini``
^^^^^^^^^^^
The configuration file for Tox.

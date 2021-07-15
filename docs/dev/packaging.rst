===============================================================================
Packaging and Dependencies
===============================================================================
In order to distribute a ready-to-use package to others via the Python Package
Index and ``conda-forge``, we need to encapsulate it with some metadata and
define its dependencies. When we first packaged up PUDL Python packaging systems, they
were a bit of a mess. Changes to the Python packaging & build system implemented
as a result of :pep:`517` and :pep:`518` have improved the available options,
and we should look at using a simpler more modern setup. The online
`Python Packages <https://py-pkgs.org/>`__ book is a great guide to current
best / better practices.

``setup.py``
^^^^^^^^^^^^

The ``setup.py`` script in the top level of the repository coordinates the
packaging process using :mod:`setuptools`, a part of the Python standard
library. ``setup.py`` is really just a single function call to
:func:`setuptools.setup`, and the parameters of that function are
metadata related to the Python package. Most of them are relatively self
explanatory -- like the name of the package, the license it's being released
under, search keywords, etc. -- but a few are more arcane:

* ``use_scm_version``: Instead of having a hard-coded version that's stored in
  the repository somewhere, handed off to the packaging script, and often out
  of date, pull the version from the source code management (SCM)
  system, in our case git (and Github). To make a release, we will first need
  to `tag a particular revision <https://help.github.com/en/articles/creating-releases>`__ in ``git``
  with a version like ``v0.1.0``.

* ``python_requires='>=3.8'``: Specifies what versions of Python the package is
  expected to run on. In this case, it's anything greater than or equal to 3.8.

* ``setup_requires=['setuptools_scm']``: What *other* packages need to be
  installed in order for the packaging script to run? Because we are obtaining
  the package version from our SCM (git/Github), we need the special package
  that lets us do that magic:
  `setuptools_scm <https://github.com/pypa/setuptools_scm>`__. This
  automatically generated version number can then be accessed in the package
  metadata, as is done our top-level ``__init__.py`` file:

  .. code-block:: python

      __version__ = pkg_resources.get_distribution(__name__).version

  This is admittedly convoluted.

* ``install_requires``: lists all the other packages that need to be installed
  before ``pudl`` can be installed. These are our package dependencies. This
  list plays a role similar to the ``environment.yml`` file in the main
  ``pudl`` repository, but it depends on ``pip`` not ``conda`` -- in the
  packaging system we do not have access to ``conda``. It turns out this makes
  our lives difficult because of the kind of Python packages we depend on. More
  on this below.

* ``extras_require``: a dictionary describing optional packages that can
  be conditionally installed depending on the expected usage of the install.
  For now, this is mostly used in conjunction with Tox to ensure that the
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
  package. In our case, this is all the stuff inside ``package_data`` including
  example settings files, metadata, glue, etc.

* ``entry_points``: This parameter tells the packaging what executable scripts
  should be installed on the user's system and which modules:functions
  implement those scripts.

``MANIFEST.in``
^^^^^^^^^^^^^^^
In addition to generating a version number automatically based on our git
repository, ``setuptools_scm`` pulls every single file tracked by the
repository and every other random file sitting in the working repository
directory into the distribution. This is... not what we want. ``MANIFEST.in``
allows us to specify in more detail which files should be included and
excluded. Mostly, we are just including the python package and supporting data that
exist under the ``src/pudl`` directory.

``pyproject.toml``
^^^^^^^^^^^^^^^^^^
The adoption of :pep:`517` and :pep:`518` has opened up the possibility of
using build and packaging systems besides :mod:`setuptools`. The new system
uses ``pyproject.toml`` to specify the build system requirements.

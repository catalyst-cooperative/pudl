===============================================================================
Project Management
===============================================================================

The people working on PUDL are distributed all over North America.  Collaboration takes
place online. We make extensive use of Github's build int project management tools and
we work in public. You can follow our progress in our
`GitHub Projects <https://github.com/orgs/catalyst-cooperative/projects/9>`__

-------------------------------------------------------------------------------
Issues and Project Tracking
-------------------------------------------------------------------------------
We use `Github issues <https://github.com/catalyst-cooperative/pudl/issues>`__ to
track bugs, enhancements, support requests, and just about any other work that goes
into the project. Try to make sure that issues have informative tags so we can find
them easily.

-------------------------------------------------------------------------------
Our GitHub Workflow
-------------------------------------------------------------------------------

* We have 2 persistent branches: **main** and **dev**.
* We create temporary feature branches off of **dev** and make pull requests to
  **dev** throughout our 2 week long sprints.
* At the end of each sprint, or any time a particularly significant feature has
  been merged in and the nightly builds are passing, **dev** is merged into **main**.

-------------------------------------------------------------------------------
Pull Requests
-------------------------------------------------------------------------------

* Before making a PR, make sure the tests run and pass locally, including the
  code linters and pre-commit hooks. See :ref:`linting` for details.
* Don't forget to merge any new commits to the **dev** branch into your feature
  branch before making a PR.
* If for some reason the continuous integration tests fail for your PR, try and
  figure out why and fix it, or ask for help. If the tests fail, we don't want
  to merge it into **dev**. You can see the status of the CI builds in the
  `GitHub Actions for the PUDL repo
  <https://github.com/catalyst-cooperative/pudl/actions>`__.
* Please don't decrease the overall test coverage -- if you introduce new code,
  it also needs to be exercised by the tests. See :doc:`testing` for
  details.
* Write good docstrings using the `Google format
  <https://www.sphinx-doc.org/en/master/usage/extensions/example_google.html#example-google>`__
* Pull Requests should update the documentation to reflect changes to the
  code, especially if it changes something user-facing, like how one of the
  command line scripts works.

-------------------------------------------------------------------------------
Releases
-------------------------------------------------------------------------------

* The PUDL data processing pipeline isn't intended to be used as a library that other
  Python packages depend on. Rather, it's an end-use application that produces data
  which other applications and analyses can consume. Because of this, we no longer
  release installable packages on PyPI or ``conda-forge``.
* Periodically, we tag a versioned release on **main** using the date, like
  ``v2023.07.15``. This triggers a snapshot of the repository being
  `archived on Zenodo <https://zenodo.org/doi/10.5281/zenodo.3404014>`__.
* The nightly build outputs associated with any tagged release will also get archived
  `on Zenodo here <https://zenodo.org/doi/10.5281/zenodo.3653158>`__
  and be made available longer term in the
  `AWS Open Data Registry <https://registry.opendata.aws/catalyst-cooperative-pudl/>`__.

-------------------------------------------------------------------------------
User Support
-------------------------------------------------------------------------------
We don't (yet) have funding to do user support, so it's currently all community
and volunteer based. In order to ensure that others can find the answers to
questions that have already been asked, we try to do all support in public
using `Github Discussions <https://github.com/orgs/catalyst-cooperative/discussions>`__.

===============================================================================
Project Management
===============================================================================

The people working on PUDL are distributed all over North America.
Collaboration takes place online. We make extensive use of Github's project
management tools as well as `Zenhub <https://www.zenhub.com>`__ which provides
additional features for sprint planning, task estimation, and progress reports.

-------------------------------------------------------------------------------
Issues and Project Tracking
-------------------------------------------------------------------------------
We use `Github issues <https://github.com/catalyst-cooperative/pudl/issues>`__ to
track bugs, enhancements, support requests, and just about any other work that goes
into the project. Try to make sure that issues have informative tags so we can find
them easily.

We use Zenhub Sprints, Epics, and Releases to track our progress. These won't be
visible unless you have the `ZenHub browser extension
<https://www.zenhub.com/extension>`__ installed.

-------------------------------------------------------------------------------
GitHub Workflow
-------------------------------------------------------------------------------
* We have 2 persistent branches: **main** and **dev**.
* We create temporary feature branches off of **dev** and make pull requests to
  **dev** throughout our 2 week long sprints.
* At the end of each sprint, assuming all the tests are passing, **dev** is
  merged into **main**.

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
* Periodically, we tag a new release on **main** and upload the packages to
  the Python Package Index and `conda-forge <https://conda-forge.org/>`__.
* Whenever we tag a release on Github, the repository is archived on `Zenodo
  <https://zenodo.org>`__ and issued a DOI.
* For some software releases we archive processed data on Zenodo along with a
  Docker container that encapsulates the necessary software environment.

-------------------------------------------------------------------------------
User Support
-------------------------------------------------------------------------------
We don't (yet) have funding to do user support, so it's currently all community
and volunteer based. In order to ensure that others can find the answers to
questions that have already been asked, we try to do all support in public
using Github issues.

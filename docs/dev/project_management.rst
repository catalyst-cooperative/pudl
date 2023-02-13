===============================================================================
Project Management
===============================================================================

The people working on PUDL are distributed all over North America.
Collaboration takes place online. We make extensive use of Github's project
management tools as well as `Zenhub <https://www.zenhub.com>`__ which provides
additional features for sprint planning, task estimation, and progress reports.

-------------------------------------------------------------------------------
Agile Scrum Kanban Iteration SDLC Process
-------------------------------------------------------------------------------

We merge code to ``dev`` continuously, but plan work in two-week sprints.

We track work as items (GitHub issues and PRs) in a Catalyst-wide Github Project.

During each sprint, people on each team pull from a team-specific list of
prioritized & scoped items.

We have 3 meetings per sprint:

- planning meeting: overall goal is "get a good idea of what work is coming up
  in the sprint"

  - mark items with a new iteration

    - enough that that we feel like there's enough work to do

    - few enough that we don't feel overwhelmed

- mid-sprint check-in: overall goal is "given this sprint's status, prioritize
  items for the next sprint"

  - make sure item status is up-to-date

  - identify items that need clarification / re-scoping

  - identify items that we'd like to pull into the backlog

- review: overall goal is "prepare prioritized items for the next sprint"

  - make sure item status is up-to-date

  - for item in backlog: if item is not ready for development, assign 1 or 2
    people to flesh it out

An issue is considered "ready for development" if has:

- an assigned team (via team label)

- a concrete set of criteria that define the scope in the issue description

- a priority

- a size

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

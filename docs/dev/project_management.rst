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

During each sprint, people on each team pull from a list of prioritized & scoped
items.

Each team has its own independent sprint process.

We have 2 or 3 meetings per sprint:

- sprint planning: overall goal is "get a good idea of what work is coming up
  in the sprint"

  - mark items with a new sprint

    - enough that that we feel like there's enough work to do

    - few enough that we don't feel overwhelmed

- (Inframundo only - other teams do this asynchronously): mid-sprint check-in:
  overall goal is "given this sprint's status, prioritize items for the next
  sprint"

  - make sure item status is up-to-date (this can be done asynchronously and
    just confirmed in-meeting)

  - identify items that need clarification / re-scoping

  - pull items from the icebox into the backlog

- sprint review: overall goal is "prepare prioritized items for the next sprint"

  - make sure item status is up-to-date (this can be done asynchronously and
    just confirmed in-meeting)

  - for item in backlog: if item is not ready for development, assign 1 or 2
    people to flesh it out

An item is considered "ready for development" if has:

- an assigned team (via GitHub label), so the issue will be picked up in
  *someone*'s sprint process

- a concrete set of criteria that define the scope in the issue description, so
  we can stop when we want to and also discuss cutting scope productively

- a priority field in the project (Urgent, High, Medium, Low), so we know which
  things to work on first

- a size field in the project (X-Large, Large, Medium, Small, Tiny), so we know
  how much time we think an item will take & how likely it is to grow beyond
  our expectations.

  - we aim for our items to be "Medium" at most - anything larger should
    be broken into smaller pieces

To focus our efforts and find bottlenecked issues, we impose a work-in-progress
limit (WIP) of N + 1 open issues per each in-flight status ("in progress", "in
review"). Here, N is "the number of people on the team" If we have too many
issues in progress or in review, we try to move those issues along instead of
starting work on a new issue. Otherwise, we try to work on the highest priority
item that is ready for development.

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

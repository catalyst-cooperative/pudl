===============================================================================
Managing External Contributions
===============================================================================

.. note::

    Looking to contribute to PUDL? The guidelines for external contributors can be found at
    :doc:`../CONTRIBUTING`.

-------------------------------------------------------------------------------
Overview
-------------------------------------------------------------------------------

This document outlines norms, practical tips, and expectations for internal
Catalyst developers supporting external community contributions.

-------------------------------------------------------------------------------
Creating 'good first issues'
-------------------------------------------------------------------------------

The 'good first issue' label on a Github issue is used to identify issues well-suited
for first-time contributors to the PUDL repository. Issues with this label are
compiled by Github into a `contribution page <https://github.com/catalyst-cooperative/pudl/contribute>`__,
indexed on sites such as `Climate Triage <https://climatetriage.com/>`__.

To be a truly good first issue, an issue should:

* be a small discrete task with clear checkpoints and well-defined outcomes
  (e.g., create one new core table, not 'integrate this dataset').
* be low context: the issue should not touch multiple complex parts of the
  code (e.g., harvesting) or require knowledge of existing bespoke workarounds in
  our code.
* be relatively easy to test and review. Relatedly, the issue should have
  constrained downstream impacts: e.g., a change to the I/O manager we use for all
  tables is probably not a great good first issue.
* have a clear implementation solution in mind: while soliciting external feedback on
  implementation design can be useful, an issue which requires us to do further research
  and decide what to do, or to weigh different options as a team is a poor choice for a
  good first issue.
* have a satisfying outcome - a new table, a bug fix, a performance improvement!
  We value our contributor's time and want them to get to work on exciting stuff,
  not chores.
* have no active blockers: while you can write up an issue to be an eventual good first
  issue, you should avoid applying the label until the blocking issue is resolved.
* not be highly time sensitive: data updates, breaking issues, and other tasks
  that need to happen on a predictable timeline are best handled by Catalyst developers.

To create a good first issue, use the issue template on Github to get started!

-------------------------------------------------------------------------------
Guidelines for contributor review
-------------------------------------------------------------------------------

Contributor project management
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When you notice a new PR, flag it in the #comdev/contributor-PRs channel in Zulip.
If nobody has volunteered to take on review, it will be assigned at the next community
development meeting. From that point on, we should aim to maintain continuity of
reviewer as much as possible for that given PR.

Contributor PRs should be tracked in the `Community Contributions <https://github.com/orgs/catalyst-cooperative/projects/19>`__
project board on Github.

Responsiveness
^^^^^^^^^^^^^^

We aim to respond to new issues, discussion posts and PRs from contributors within one
week at most.

* Discussion posts: Initial responses should attempt to answer the question or provide
  additional resources (e.g., office hours).
* Issues: Initial responses should communicate prioritization, clarify any necessary
  information, and provide an expected timeline of response if one is known.
* PRs: Initial responses should assess whether the PR is ready for review,
  address any high-level questions or missing information, and provide an expected
  timeline for review. If the assigned internal reviewer takes more than 2 weeks to
  provide a review, we should check-in and consider re-assigning the review to someone
  else.

Communication guidelines
^^^^^^^^^^^^^^^^^^^^^^^^

When reviewing an external contributor's PR, a reviewer should:

* Assume good intent.
* Favor tasklists and clear steps as much as possible, over vague requests such as
  "normalize the data".
* Provide pointers to relevant code, particularly where we've already implemented
  helper methods. E.g., rather than say "add this field to our metadata", ask someone
  to "add this field to the FIELD_METADATA dictionary in pudl.src.metadata.fields".
* Be clear about what's blocking a merge or isn't. Describe changes as non-blocking when
  they are either our personal preferences or changes we'd be willing to take over
  ownership of. We recommend using the `labels <https://conventionalcomments.org/#labels>`__
  from Conventional Comments and referring contributors to this as a reference.
* Give people the option to stretch their skills (e.g., take on an additional fix,
  learn a new skill), while also providing an out. For instance, "this is the 3rd
  time we've done something similar in this dataset's transformation - any interest in
  refactoring these into one unified function?".
* Recognize that "data cleaning" and "schema finessing" are different skillsets, and
  we tend to be pickier about the latter. Fine-tuning field descriptions or table
  metadata may be a natural point at which to takeover a contributor PR.

Unresponsive contributors
^^^^^^^^^^^^^^^^^^^^^^^^^

Our general protocol is to ping after a month of non-activity, and to close
after a further two weeks if no response. We can always re-open a PR if need be.

-------------------------------------------------------------------------------
Common contributor management decisions
-------------------------------------------------------------------------------

The following guidelines cover several decision points that commonly arise when tending
to PRs from external contributors:

* Deciding when to merge
* Deciding when to take over
* Deciding when to close without merging

When to merge
^^^^^^^^^^^^^

The standard of code quality for external contributors should be no higher than
the one we use internally, and the one we use internally is probably too high!
All we really need from a proposed change is to know that it is (1) making
something better without (2) making something worse.

This decision follows from 🐥 taking small steps.

We know that something is better if:

* It addresses all or part of an existing Github issue.
* It sets up an acceptable pausing point that someone else could pick up without
  requiring extensive context.

  * Example: persisting an intermediate asset

* It can be understood and expanded on by others. This includes writing clearly
  understood code, using well-chosen variable names, adding function definitions and
  in-line comments, and leaving enough context in the issue and PR to make it possible
  for others to expand on this work.
* It has sufficient tests to be believable and guard against likely future changes.

  * Example: defending against load-bearing assumptions, but not checking every
    possible corner case

We know something is worse if:

* It makes a big mess in the code or output.

  * Example: using custom identifiers when something already in PUDL would do

* It causes previously-passing tests to fail.
* It lacks a clearly-explained purpose.

  * Example: dropping pre-existing data outputs without justification

When to take over
^^^^^^^^^^^^^^^^^

Sometimes the last few changes to make an external PR ready to merge are better
off if completed by Catalyst. In these cases, we can "take over" the PR to push
it over the line, either by pushing to the external branch directly or merging to a
non-main Catalyst branch and opening an internal PR.

We would take over if:

* The contributor has become unresponsive AND the remaining effort is small
  relative to the value of the proposed change

  * Careful: a partial implementation from an external contributor is not enough
    reason to pursue a change that lies outside our area of interest
  * If more than 25% of the expected work remains, consider proposing it as a
    special project or in the next quarterly plan instead

* The proposed change has become urgent, beyond the contributor's available pace

  * Example: the change is blocking other necessary work

* The remaining tasks require a large amount of internal context of limited
  value to the contributor, such as:

  * Harvesting
  * Schema nitpicking
  * Writing dbt or other obscure tests
  * Resolving alembic merges
  * Aligning code style to the rest of the repo, beyond what's available to the
    contributor in the dev guide

* The proposed change has gone 3-4 rounds of review and the remaining effort is small

  * This is purely about diminishing returns. Good reviews take time, each round
    provides less benefit to both the contributor and to us. We'd rather free
    the contributor to work on something else than go 10 rounds on nits.

When to close without merging
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Occasionally we get contributions that are not aligned with our goals. While it
is important to start out by assuming good faith, it is appropriate to close PRs
that lie outside of our ability to manage.

These situations require a lot of individual judgement, and it is always okay to
ask another team member for support if you are not sure how to proceed.

We would close a contributor PR if:

* The contribution does not address the issue they are trying to close, in a way
  that is not trivial to fix and suggests they haven't paid much attention to
  the issue.

  * Particularly relevant for first-time contributors

* The contribution is self-described as being opened by an agent.
* The issue being addressed was not intended for community contribution.

  * Example: a half-baked idea that requires more internal discussion

* The contribution does not address any issue and was not expressly invited by us.
* The contributor has not responded to our messages for 6 weeks.

  * They can always re-open if they are still interested.

* The contributor does not have a functioning development environment and cannot
  run the ETL, after repeated office hours or attempts to assist.
* The contribution requires ongoing maintenance beyond our capacity to fund.

  * Example: hand-compiled mappings for data subject to frequent updates

-------------------------------------------------------------------------------
Github incantations
-------------------------------------------------------------------------------

To successfully check out a contributor's PR from a fork, run the following:

.. code-block:: bash

    # Add the user as a remote (replace user-name with the actual username)
    git remote add user-name https://github.com/user-name/pudl.git
    # Fetch the latest version of that user's forked repository
    git fetch user-name
    # List all your remotes to verify that it worked
    git remote -v
    # Checkout their branch to a local branch.
    # It's simplest if your local branch name matches the forked branch name.
    git checkout -b branch-name user-name/branch-name
    # To check that you're in the right place and up to date, you can run
    # git log to verify the most recent commits on the branch.
    git log

Now you can pull, commit, etc. as per usual. Though we generally encourage contributors
to make changes to their own branch, there are some cases in which we want to push
directly to their branch for them (e.g., a PR is abandoned at 98% completion).

To push your local branch directly to a contributor's PR, run the following:

.. code-block:: bash

    git push user-name branch-name:branch-name
    # If you have a different local branch name from the remote, it
    # should instead look like this
    git push user-name local-branch-name:remote-branch-name

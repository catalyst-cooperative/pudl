===============================================================================
Managing External Contributions
===============================================================================

-------------------------------------------------------------------------------
Overview
-------------------------------------------------------------------------------

This document outlines norms, practical tips, and expectations for internal
Catalyst developers reviewing external community contributions.

-------------------------------------------------------------------------------
Guidelines for contributors
-------------------------------------------------------------------------------

The guidelines for external contributors can be found at :doc:`../CONTRIBUTING`.

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

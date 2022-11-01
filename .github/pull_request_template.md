# PR Process Overview

* We require a code review before merging PRs into the `dev` branch.
* It is much easier to do timely code reviews on smaller chunks of code.  We try to keep
  PRs under 500 lines of code.
* If you're doing a bigger refactor or adding a large subsystem, it may be better to
  create a persistent branch off of `dev` and then merge smaller, incremental PRs into
  that persistent branch, getting each of them reviewed. Then at the end the persistent
  branch can be merged into `dev` with minimal review.
* Another option for larger changes is to create a draft PR as soon as you have pushed
  any changes to your branch, and get the attention of a potential reviewer early on,
  who can review the draft PR periodically as it evolves over time. This can be very
  helpful in getting design feedback and understanding how your changes will interact
  with the rest of the PUDL codebase. In this case, limiting the scope of each commit to
  a single change makes it easier to review incrementally.
* How quickly we can review a PR will depend on how large and complex it is, and how
  busy we are, but ideally we strive to get an initial review done within a week. If
  there are going to be delays, we should at least comment on the PR to let you know the
  situation.
* If you believe you've addressed a reviewer's comments, respond with a brief note and
  mark the comment resolved. If further discussion is requried respond and do not
  resolve the comment.
* Before a PR is merged all reviewer comments should be resolved. If a reviewer doesn't
  feel that their comment has been sufficiently addressed, they may unresolve a comment.
* Be careful not to accidentally "start a review" when responding to comments! If this
  does happen, don't forget to submit the review you've started so the other PR
  participatns can see your comments (they are invisible if marked "Pending").
* In the period after an initial review when there is significant back-and-forth with
  the reviewer deciding what changes should actually be made, there should probably be
  daily interaction. If significant changes are required, it's usually best to request
  another review after those changes have been made.

# PR Checklist

Before requesting a review of your pull request, please make sure you've done the
following:

* [ ] Merge the most recent version of `dev` (or the appropriate upstream branch) into
      your branch and resolved any merge conflicts. You may need to do this several
      times over the course of a PR as `dev` changes frequently.
* [ ] Verify that all of the CI checks on your PR are passing. See
      [Running Tests with Tox](https://catalystcoop-pudl.readthedocs.io/en/latest/dev/testing.html#running-tests-with-tox)
      for details on how to run the full test suite locally if you need to debug a
      particular failure.
* [ ] Ensure that the docstrings for any new modules, classes, functions, or methods are
      descriptive enough for developers and users to understand your code.
* [ ] If you expanded data coverage or changed the outputs, ensure that the full
      [data validation tests](https://catalystcoop-pudl.readthedocs.io/en/latest/dev/testing.html#data-validation)
      pass locally on a fresh DB.
* [ ] If you've added new functions or classes, ensure that they have at least basic
      unit tests.
* [ ] If you've added new analyses, make sure they include defensive sanity checks that
      will catch unexpected data issues.
* [ ] Update the
      [release notes](https://catalystcoop-pudl.readthedocs.io/en/latest/release_notes.html)
      to reflect your changes. Make sure to reference the PR and any related issues.
* [ ] Do your own review of the PR. Add comments highlighting areas where you have
      questions you'd like reviewers to answer, known issues, solutions you're
      unsatisfied with, or other things that deserve special attention from the
      reviewer.

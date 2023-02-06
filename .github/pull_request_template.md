<!--

Making a PUDL Pull Request

Before making a PR you may want to check out our:

* contributing guidelines: https://catalystcoop-pudl.readthedocs.io/en/latest/CONTRIBUTING.html
* code of conduct: https://catalystcoop-pudl.readthedocs.io/en/latest/code_of_conduct.html
* development process: https://catalystcoop-pudl.readthedocs.io/en/latest/dev/index.html

## PR Process Overview

* PRs have to get an approving review before merging into their development branch.
* Most PRs should be made against the `dev` branch, unless they are part of some larger ongoing refactoring, in which case there will be a persistent development branch for that work.
* It is much easier to do timely code reviews on smaller chunks of code. We try to keep PRs under 500 lines of code.
* Draft PRs are a good way to get early feedback on designs or several incremental commits that will add up to larger changes. If you want a review of a draft PR, make sure you contact the reviewer directly or mention their username in the PR comment, so they get a notification.
* How quickly we can review a PR will depend on how large and complex it is, and how busy we are, but ideally we strive to get an initial review done within a week. If there are going to be delays, we should at least comment on the PR to let you know the situation.
* If you believe you've addressed a reviewer's comments, respond with a brief note and mark the comment resolved. If further discussion is requried respond and do not resolve the comment.
* Before a PR is merged all reviewer comments should be resolved. If a reviewer doesn't feel that their comment has been sufficiently addressed, they may unresolve a comment.
* Be careful not to accidentally "start a review" when responding to comments! If this does happen, don't forget to submit the review you've started so the other PR participatns can see your comments (they are invisible to others if marked "Pending").
* In the period after an initial review when there is significant back-and-forth with the reviewer deciding what changes should actually be made, there should probably be daily interaction. If significant changes are required, it's usually best to request another review after those changes have been made.

Feel free to delete the commented-out parts of the template before submitting the PR.

-->

# PR Overview

<!--

Include a short narrative summary of what's going on in the PR. This can be a bulleted list. You might want to include:

* What are you changing and why?
* Are there any known unsolved problems remaining in the PR?
* Is there anything that you want a reivewer to pay particular attention to?
* What kind of feedback are you looking for on the PR?

-->

# PR Checklist

- [ ] Merge the most recent version of the branch you are merging into (probably `dev`).
- [ ] All CI checks are passing. [Run tests locally to debug failures](https://catalystcoop-pudl.readthedocs.io/en/latest/dev/testing.html#running-tests-with-tox)
- [ ] Make sure you've included good docstrings.
- [ ] For major data coverage & analysis changes, [run data validation tests](https://catalystcoop-pudl.readthedocs.io/en/latest/dev/testing.html#data-validation)
- [ ] Include unit tests for new functions and classes.
- [ ] Defensive data quality/sanity checks in analyses & data processing functions.
- [ ] Update the [release notes](https://catalystcoop-pudl.readthedocs.io/en/latest/release_notes.html) and reference reference the PR and related issues.
- [ ] Do your own explanatory review of the PR to help the reviewer understand what's going on and identify issues preemptively.

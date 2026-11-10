# Contributing to PUDL

Welcome! We’re excited that you’re interested in contributing to the Public Utility
Data Liberation effort!

We need lots of help with [User feedback](#user-feedback), we welcome [Code contributions](#code-contribs), and
it would be great to [Connect us with other organizations](#connect-orgs) that we can work with. Financial support via
our [PUDL Sustainers](#pudl-sustainers) program is always welcome.

<a id="pudl-sustainers"></a>

## PUDL Sustainers

If you use or appreciate PUDL data and want to help ensure that it continues to be
openly and freely available to the public (and yourself) please consider becoming a PUDL
Sustainer. See the [PUDL project profile on Open Collective](https://opencollective.com/pudl) for more information. Contributions of any size are
appreciated. Sustainers at higher tiers are invited to help guide the project’s
priorities in our quarterly planning process.

## Code of Conduct

Please make sure you review our [code of conduct](code_of_conduct.md), which is
based on the [Contributor Covenant](https://www.contributor-covenant.org/). We
want to make the PUDL project welcoming to contributors with different levels of
experience and diverse personal backgrounds.

<a id="user-feedback"></a>

## User feedback

PUDL’s goal is to help people use data to make change in the US energy landscape.
As such, it’s critical that we understand our users’ needs! [GitHub Discussions](https://github.com/orgs/catalyst-cooperative/discussions) is our main forum
for all this. Since it’s publicly readable, any conversation here can
potentially benefit other users too!

We’d love it if you could:

* Tell us what problems you’re running into, in the [Help Me!](https://github.com/orgs/catalyst-cooperative/discussions/categories/help-me)
  discussion board
* Tell us about what data you’re looking for by opening an [issue](https://github.com/catalyst-cooperative/pudl/issues/new?assignees=&labels=new-data&projects=&template=new_dataset.md&title=)
* Tell us what you’re trying to do with PUDL data in [this thread](https://github.com/orgs/catalyst-cooperative/discussions/3105)
* [File bug reports](https://github.com/catalyst-cooperative/pudl/issues/new?template=bug_report.md) on Github.
* Tell us what you’d like to see in PUDL in the [Ideas](https://github.com/orgs/catalyst-cooperative/discussions/categories/ideas)
  discussion board

<a id="code-contribs"></a>

## Code contributions

#### IMPORTANT
Already have a dataset in mind?

If you **need data that’s not in PUDL**, [open an issue](https://github.com/catalyst-cooperative/pudl/issues/new?assignees=&labels=new-data&projects=&template=new_dataset.md&title=)
to tell us more about it!

If you’ve **already spent a bunch of time wrangling a dataset**, we welcome
“knowledge contributions” in our [pudl-knowledge](https://github.com/catalyst-cooperative/pudl-knowledge) repository!

If you’re **looking to help us integrate a specific dataset into PUDL**, find us at
[office hours](https://calend.ly/catalyst-cooperative/pudl-office-hours) and we
can talk through next steps.

<a id="llm-policy"></a>

### LLM use policy

1. **All PRs, internal and external, will be reviewed by a human.**

> * 🏃 We don’t want the code to grow faster than we can understand it.
> * 🧹 This is particularly important for data cleaning and subtle
>   domain-informed decision making.

> That means we need to optimize work for ease of human review:

> * 💞 Reviewers are humans - respect their time.
> * Review is the bottleneck, so let’s keep it going as smoothly as
>   possible.
1. **When reviewing and self-reviewing, make a human synthesis of the
   work - what changed? Why? What did you learn?**

> * 🏃 We need to make sure we understand what’s going into the
>   codebase.
> * 🧹 We need to make sure domain-specific changes are well-considered.
1. **When making a PR with some LLM-generated content, briefly describe
   the usage of LLMs so we know what to expect.** Examples:

> * “I used an LLM to generate these tests, and cleaned them up by hand.”
> * “I talked through my plan with an LLM, revised it, and then had it
>   implement the plan.”
1. **🗽In order to avoid vendor lock-in, we will reject additions to
   Catalyst LLM tooling that require the use of one particular model
   or harness.** We will actively engage with open tooling and openly
   licensed models to avoid relying on proprietary solutions.

### Your first contribution

**Setup**

You’ll need to fork this repository and get the
[dev environment set up](https://docs.catalyst.coop/pudl/en/nightly/dev/dev_setup.html).

**Pick an issue**

* Look for issues with the [good first issue](https://github.com/catalyst-cooperative/pudl/issues?q=is%3Aissue+is%3Aopen+label%3Agood-first-issue)
  tag in our [Community Kanban Board](https://github.com/orgs/catalyst-cooperative/projects/9/views/19). These
  are issues that don’t require a ton of PUDL-specific context, and are
  relatively tightly scoped.
* Comment on the issue and tag `@catalyst-cooperative/com-dev` (our Community
  Development Team) to let us know you’re working on it. Feel free to ask any
  questions you might have!
* Once you have an idea of how you want to tackle this issue, write out your
  plan so we can guide you around obstacles in your way! Post a comment outlining:
  * what steps have you broken this down into?
  * what is the output of each step?
  * how will one know that each step is working?
* Once you’ve talked through your plan with someone from Catalyst, go forth and
  develop!

**Work on it!**

* Make a branch on your fork and open a draft pull request (PR) early so we can
  discuss concrete code! **Set the base branch to \`\`main\`\`.** Please don’t wait
  until it’s all polished up - it’s much easier for us to help you when we can
  see the code evolve over time.
* Please make sure to write tests and documentation for your code - if you run
  into trouble with writing tests, let us know in the comments and we can help!
  We automatically run the test suite for all PRs, but some of those will have
  to be manually approved by Catalyst members for safety reasons.
* **Try to keep your changes relatively small:** stuff happens, and one’s
  bandwidth for volunteer work can fluctuate frequently. If you make a bunch of
  small changes, it’s much easier to pause on a project without losing a ton of
  context. We try to keep PRs to **less than 500 lines of code.**

**Get it merged in!**

* Turn the draft PR into a normal PR and tag `@catalyst-cooperative/com-dev`
  in a comment. We’ll try to get back to you within a few days - the
  smaller/simpler the PR, the faster we’ll be able to get back to you.
* The reviewer will leave comments - if they request changes, address their
  concerns and re-request review.
* There will probably be some back-and-forth until your PR is approved - this
  is normal and a sign of good communication on your part! Don’t be shy about
  asking us for updates and re-requesting review!
* Don’t accidentally “start a review” when responding to comments! If this does
  happen, don’t forget to submit the review you’ve started so the other PR
  participants can see your comments (they are invisible to others if marked
  “Pending”).

### Next contributions

Hooray! You made your first contribution! To find another issue to tackle, check
out the [Community Kanban board](https://github.com/orgs/catalyst-cooperative/projects/9/views/19) where
we’ve picked out some issues that are

* useful to work on
* unlikely to become super time-sensitive
* have some context, success criteria, and next steps information.

Pick one of these and follow the contribution flow above!

### Contributor survey

We’d love your feedback on working with PUDL. Please consider taking ~10 minutes
to fill out our
[contributor survey](https://docs.google.com/forms/d/e/1FAIpQLSd8bbncVEKLt-EDktTAgL6AJt_fmRXFkhE955Ax0w6QIXClEg/viewform?usp=header)
so we can improve the process for future contributors!

<a id="connect-orgs"></a>

## Connect us with other organizations

For PUDL to make a bigger impact, we need to find more people who need the data.
Here’s how you can help:

* Cite PUDL using [DOIs from Zenodo](https://zenodo.org/communities/catalyst-cooperative/) if you use the
  software or data in your own published work.
* Point us toward appropriate grant funding opportunities and meetings where
  we might present our work.
* Point us at interesting publications related to open energy data, open source
  energy system modeling, how energy policy can be affected by better data, or
  open source tools we should check out.
* Share your Jupyter notebooks and other analyses that use PUDL.
* [Hire Catalyst](https://catalyst.coop/hire-catalyst/) to do analysis for
  your organization using the PUDL data – contract work helps us self-fund
  ongoing open source development.

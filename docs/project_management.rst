===============================================================================
Project Management
===============================================================================

The people working on PUDL are distributed all over North America.
Collaboration takes place online. We make extensive use of Github's project
management tools, as well as `Zenhub <https://www.zenhub.com>` which provides
additional features within the context of a public facing Github project.

-------------------------------------------------------------------------------
Issues and Project Tracking
-------------------------------------------------------------------------------

We use `Github issues <https://github.com/catalyst-cooperative/pudl/issues>`_
to track bugs, enhancements, support requests, and just about any other work
that goes into the project. The issues are organized into several different
streams of work, using `Github projects
<https://github.com/catalyst-cooperative/pudl/projects>`_

We are happy to accept pull requests that improve our existing code, expand the
data that's available via PUDL, and and make our documentation more readable
and complete. Feel free to report bugs, comment on existing issues, suggest
other data sources that might be worth integrating, or ask questions about how
to use PUDL if you can't find the answer in our documentation.

-------------------------------------------------------------------------------
Release Management
-------------------------------------------------------------------------------

We are developing and releasing software, but we're also using that software to
process and publish data. Our goal is to make the data pipeline as easily and
reliably replicable as possible.

Whenever we tag a release on Github, the repository is archived on `Zenodo
<https://zenodo.org>`_ and issued a DOI. Then the package is uploaded to the
Python Package Index for distribution. Our goal is to make a software release
at least once a quarter.

Data releases will also be archived on Zenodo, and consist of a software
release, a collection of input files, and the resulting data packages. The goal
is to make the data package output reproducible given the archived input files
and software release, with a single command. Our goal is to make data releases
quarterly as well.

-------------------------------------------------------------------------------
User Support
-------------------------------------------------------------------------------

We don't (yet) have funding to do user support, so it's currently all community
and volunteer based. In order to ensure that others can find the answers to
questions that have already been asked, we try to do all support in public
using Github issues.

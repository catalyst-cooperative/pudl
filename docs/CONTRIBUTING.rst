===============================================================================
Contributing to PUDL
===============================================================================

PUDL is an open source project that has been supported by a combination of
volunteer efforts, grant funding, and reinvestment of surplus income by
Catalyst Cooperative. See our :doc:`/acknowledgments` for more details.

The work is currently being coordinated by the members of
`Catalyst Cooperative <https://catalyst.coop>`_. PUDL is meant to serve a wide
variety of public interests including academic research, climate advocacy, data
journalism, and public policy making.

For more on the motivation and history of the project, have a look at
:doc:`this background info <background>`. Please also be sure to review our
:doc:`code of conduct <code_of_conduct>`, which is based on the
`Contributor Covenant <https://www.contributor-covenant.org/>`__.
We want to make the PUDL project welcoming to contributors with different
levels of experience and diverse personal backgrounds.

-------------------------------------------------------------------------------
How to Get Involved
-------------------------------------------------------------------------------

We welcome just about any kind of contribution to the project. Alone we'll
never be able to understand every use case or integrate all the available data.
The project will serve the community better if other folks get involved.

There are lots of ways to contribute -- it's not all about code!

  * :ref:`Ask questions on Github <aq_on_gh>` using the `issue tracker <https://github.com/catalyst-cooperative/pudl/issues>`__.
  * `Suggest new data and features <https://github.com/catalyst-cooperative/pudl/issues/new?template=feature_request.md>`__ that would be useful.
  * `File bug reports <https://github.com/catalyst-cooperative/pudl/issues/new?template=bug_report.md>`__ on Github.
  * Help expand and improve the documentation, or share example notebooks.
  * Give us feedback on overall usability -- what's confusing?
  * Tell us a story about how you're using of the data.
  * Point us at :ref:`interesting publications <bg-reading>` related to
    energy data, or energy system modeling.
  * Cite PUDL using
    `DOIs from Zenodo <https://zenodo.org/communities/catalyst-cooperative/>`__
    if you use the software or data in your own published work.
  * Point us toward appropriate grant funding opportunities and meetings where
    we might present our work.
  * Share your Jupyter notebooks and other analyses that use PUDL.
  * `Hire Catalyst <https://catalyst.coop/hire-catalyst/>`__ to do analysis for
    your organization using the PUDL data -- contract work helps us self-fund
    ongoing open source development.
  * Contribute code via
    `pull requests <https://help.github.com/en/articles/about-pull-requests>`__.
    See the :doc:`developer setup <dev/dev_setup>` for more details.
  * And of course... we also appreciate
    `financial contributions <https://www.paypal.com/cgi-bin/webscr?cmd=_s-xclick&hosted_button_id=PZBZDFNKBJW5E&source=url>`__.


-------------------------------------------------------------------------------
Find us on GitHub
-------------------------------------------------------------------------------
Github is the primary platform we use to manage the project, integrate
contributions, write and publish documentation, answer user questions, automate
testing & deployment, etc.
`Signing up for a GitHub account <https://github.com/join>`__
(even if you don't intend to write code) will allow you to participate in
online discussions and track projects that you're interested in.

.. _aq_on_gh:

Ask Questions on Github
^^^^^^^^^^^^^^^^^^^^^^^
Asking (and answering) questions is a valuable contribution!

As noted in `How to support open-source software and stay sane
<https://www.nature.com/articles/d41586-019-02046-0>`__ It's much more
efficient to ask and answer questions in a public forum because then other
users and contributors who are having the same problem can find answers without
having to re-ask the same question. The forum we're using is our `Github issues
<https://github.com/catalyst-cooperative/pudl/issues>`__.

Even if you feel like you have a basic question, we want you to feel
comfortable asking for help in public -- we (Catalyst) only recently came to
this data work from being activists and policy wonks -- so it's easy for us to
remember when it all seemed frustrating and alien! Sometimes it still does. We
want people to use the software and data to do good things in the world. We
want you to be able to access it. Using a public forum also enables the
community of users to help each other!

.. _suggest_on_gh:

Make Suggestions on GitHub
^^^^^^^^^^^^^^^^^^^^^^^^^^
Don't hesitate to open an issue with a
`feature request <https://github.com/catalyst-cooperative/pudl/issues/new?template=feature_request.md>`__,
or a pointer to energy data that needs liberating, or a reference to
documentation that's out of date, or unclear, or missing. Understanding how
people are using the software, and how they would *like* to be using the
software is very valuable, and will help us make it more useful and usable.

.. _coding_standards:

-------------------------------------------------------------------------------
Coding Standards
-------------------------------------------------------------------------------

* We are trying to keep our own code entirely written in Python.
* It's our expectation that PUDL will be run on Unix-like operating systems,
  including Linux and MacOS. We have run into issues working on Windows, and
  are intending for it to be run within Docker by most users. However, we are
  still trying to use system agnostic filesystem paths and other portable
  coding practices.
* It's currently our intent to support on the most recent widely used versions
  of Python.
* Before making a PR, make sure the tests run and pass locally, including the
  linters.
* Don't decrease the overall test coverage -- if you introduce new code it
  also needs to be exercised by the tests. See :doc:`dev/testing` for details.
* Write good docstrings, using the `Google docstring <https://www.sphinx-doc.org/en/latest/usage/extensions/example_google.html>`__ format.

.. seealso::

    * :doc:`dev/dev_setup` for instructions on how to set up the PUDL
      development environment.
    * :doc:`dev/testing` for documentation of how to run the tests.

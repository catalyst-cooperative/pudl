===============================================================================
How to Cite PUDL
===============================================================================

If you use PUDL data or software in a publication, report, or analysis, please
cite it! Citations help us demonstrate the impact of PUDL to funders, and they
help your readers find and reproduce the exact data you used. All PUDL data is
published under the `Creative Commons Attribution 4.0 International (CC-BY-4.0)
<https://creativecommons.org/licenses/by/4.0/>`__ license, which requires
attribution. All PUDL code is published under the `MIT License <https://opensource.org/license/mit>`__
which requires a copyright notice.

We recommend that you cite a :ref:`versioned release <cite-versioned-release>` of
PUDL using a version-specific DOI. If you want, you can also
:ref:`name the specific tables <cite-table>` you used or cite the :ref:`PUDL
software <cite-software>` if you reference or modify the code as part of your
analysis.

.. _cite-why-versioned:

-------------------------------------------------------------------------------
Why Cite a Versioned Release?
-------------------------------------------------------------------------------

Every versioned PUDL release is archived permanently on Zenodo with its own DOI. Anyone
reading your work can use that DOI to download exactly the data you used, even years
after you published.

Our nightly builds are useful for getting the newest data, but they are not
archived on Zenodo and don't have DOIs. Each build overwrites the previous one in
``s3://pudl.catalyst.coop/nightly/``, so the data at that location changes over time.

If you accessed the data through the ``stable`` path on S3,
it's from the most recent versioned release. If you accessed it through the
``nightly`` path on S3 or the `PUDL Data Viewer <https://data.catalyst.coop/>`__,
it's from the nightly builds.

.. _cite-dois:

-------------------------------------------------------------------------------
Concept DOIs vs. Version-specific DOIs
-------------------------------------------------------------------------------

We archive every versioned PUDL data and software release on
`Zenodo <https://zenodo.org>`__, which assigns each archive a `Digital Object
Identifier (DOI) <https://www.doi.org/>`__. Zenodo issues two kinds of DOI:

Concept DOI
    A single DOI that represents *all* versions of the PUDL data or software
    release. It always resolves to the most recent version:

    * The PUDL data release concept DOI is `10.5281/zenodo.3653158 <https://doi.org/10.5281/zenodo.3653158>`__.
    * The PUDL software release concept DOI is `10.5281/zenodo.3404014 <https://doi.org/10.5281/zenodo.3404014>`__.

Version-specific DOI
    Every individual release, such as ``v2026.11.0``, gets its own DOI that always
    resolves to exactly that version of the data or software and never changes.

.. important::

   **Use the version-specific DOI when you cite data you actually used.**

   The concept DOI points to a different version each time we publish a new
   release, so a citation that uses it won't tell your readers which data you
   analyzed. The concept DOI is useful when you want to refer to PUDL in
   general, or point someone to the latest release.

.. _cite-versioned-release:

-------------------------------------------------------------------------------
Citing a Versioned Data Release
-------------------------------------------------------------------------------

To find the version-specific DOI for a release:

#. Go to the `latest PUDL data release on Zenodo
   <https://doi.org/10.5281/zenodo.3653158>`__.
#. In the **Versions** panel on the right-hand side of the page, select the version you
   used (``View all versions`` lists older releases).
#. Copy the DOI shown on that version's page. Zenodo's **Citation** panel will also
   generate a full citation in many formats, including BibTeX, APA, and Chicago that
   you can copy and use.

**Example Citation for v2026.11.0:**

    Selvans, Z., Gosnell, C., Sharpe, A., Schira, Z., Xia, D., Belfer, E., & Mazaitis,
    K. (2026). *The Public Utility Data Liberation (PUDL) Project* (v2026.11.0) [Data
    set]. Catalyst Cooperative. https://doi.org/10.5281/zenodo.24119965

.. code-block:: bibtex

   @dataset{pudl_v2026_11_0,
     author    = {Selvans, Zane and Gosnell, Christina and Sharpe, Austen and
                  Schira, Zachary and Xia, Dazhong and Belfer, Ella and
                  Mazaitis, Kathryn},
     title     = {The Public Utility Data Liberation (PUDL) Project},
     version   = {v2026.11.0},
     year      = {2026},
     publisher = {Catalyst Cooperative},
     doi       = {10.5281/zenodo.24119965},
     url       = {https://doi.org/10.5281/zenodo.24119965},
   }

.. _cite-table:

-------------------------------------------------------------------------------
Citing a Specific Table
-------------------------------------------------------------------------------

PUDL distributes all of its tables together as a single dataset, so there is no
separate DOI for each table. To cite a specific table, cite the versioned release you
used as described above, and name the table in your text or in the citation. Use the
table's full name as it appears in the :doc:`/data_dictionaries/pudl_db`, e.g.
``out_eia__yearly_generators``. This tells readers exactly which data you used, which
matters because many PUDL tables contain similar information at different levels of
processing or aggregation.

For example, to cite the ``out_eia__yearly_generators`` table from the v2026.11.0
release:

    Selvans, Z., Gosnell, C., Sharpe, A., Schira, Z., Xia, D., Belfer, E., & Mazaitis,
    K. (2026). out_eia__yearly_generators. In *The Public Utility Data Liberation (PUDL)
    Project* (v2026.11.0) [Data set]. Catalyst Cooperative.
    https://doi.org/10.5281/zenodo.24119965

.. code-block:: bibtex

   @dataset{pudl_v2026_11_0_out_eia__yearly_generators,
     author    = {Selvans, Zane and Gosnell, Christina and Sharpe, Austen and
                  Schira, Zachary and Xia, Dazhong and Belfer, Ella and
                  Mazaitis, Kathryn},
     title     = {out\_eia\_\_yearly\_generators},
     booktitle = {The Public Utility Data Liberation (PUDL) Project},
     version   = {v2026.11.0},
     year      = {2026},
     publisher = {Catalyst Cooperative},
     doi       = {10.5281/zenodo.24119965},
     url       = {https://doi.org/10.5281/zenodo.24119965},
   }

-------------------------------------------------------------------------------
Citing the Original Data Sources
-------------------------------------------------------------------------------

PUDL is built from data published by government agencies like EIA, FERC, and EPA. When
your work relies on data from a particular source, we encourage you to credit the
original publisher as well. You can find links and documentation for each source on
the :doc:`/data_sources/index` pages.

.. _cite-software:

-------------------------------------------------------------------------------
Citing PUDL Software
-------------------------------------------------------------------------------

If you want to cite the PUDL *code*, (e.g. if you modified the data processing
pipeline or reference the code directly), you can `access a snapshot of the code
from the latest release <https://doi.org/10.5281/zenodo.3404014>`__ and follow
the same instructions as :ref:`citing the versioned release <cite-versioned-release>`.

Under the MIT license, the PUDL software is free to use and modify, but if you
choose to redistribute the PUDL software publicly you must include a notice of
the original copyright. This is found in the ``license.rst`` file distributed
with the code.

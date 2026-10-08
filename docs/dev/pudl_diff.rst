===============================================================================
PUDL Diff
===============================================================================

``pudl_diff`` compares two sets of PUDL Parquet outputs, table by table, and reports
on what changed: the schema, the row counts, and the rows themselves. We use it to
confirm that a change to the PUDL codebase, its dependencies, or its raw inputs did, or
didn't, change the data, and to see how nightly builds and stable releases differ.

It is developed in its own repository, at
`catalyst-cooperative/pudl-diff <https://github.com/catalyst-cooperative/pudl-diff>`__,
and published as the ``catalystcoop.pudl_diff`` package, which PUDL depends on. Its
`documentation <https://docs.catalyst.coop/pudl-diff/>`__ covers using the ``pudl_diff``
command and the format of its JSON report. This page covers how PUDL uses it.

Since PUDL is installed alongside it, ``pudl_diff`` uses PUDL's own definitions of the
nightly build's location, ``$PUDL_OUTPUT``, and the primary keys of PUDL's tables. By
default it compares the last nightly build against your local outputs in
``$PUDL_OUTPUT/parquet``:

.. code-block:: console

   $ pudl_diff out_eia__yearly_generators

-----------------------------------------------
PUDL Diff in the build process
-----------------------------------------------

Builds on Google Batch make PUDL Diff reports as part of the ETL, in the
``pudl_diff`` asset, which depends only on ``pudl_datapackage``. It is turned on by
``dg_nightly.yml``, and is otherwise off, since it reads whole baseline datasets.
To run it in a local ETL, set ``PUDL_DIFF_RUN=true``. It then compares against the
last nightly build, or against a dataset of your choosing if you set
``PUDL_DIFF_LEFT_ROOT`` to its root.

The baselines depend on the build's git tag:

* **Branch and nightly builds** compare against the last nightly build
  (``s3://pudl.catalyst.coop/nightly/``) and against the last stable release
  (``s3://pudl.catalyst.coop/stable/``).
* **Stable release builds** make no report in the ETL. Instead ``pudl_deploy``
  compares the prepared outputs against the previous stable release, and records the
  permanent versioned paths of both, e.g. ``s3://pudl.catalyst.coop/v2026.9.0/``, as
  the roots in the report. This is a durable record of how the data changed from
  release to release, so if it can't be made the deployment fails. Any reports carried
  in the build outputs, such as those of a nightly build being redeployed as a stable
  release, are replaced.

Each report is written to ``$PUDL_OUTPUT/pudl_diff/<left>-vs-<right>/``, named for the
git tags of the two datasets. If a dataset has several, versioned release tags
(``v20...``) are preferred to nightly build tags (``nightly-...``), and those to branch
build tags (``branch-...``). A build with no tags is named for its build ID instead.
Since the build's outputs are saved to ``gs://builds.catalyst.coop/<build-id>/``, so
are the reports. Deployments then publish them with the rest of the outputs,
in ``pudl_diff/`` and as a ``pudl_diff.zip`` archive (which is also uploaded to Zenodo
with stable releases). They aren't published to ``eel-hole``.

PUDL Diff reports made in builds and deployments compare every table row by row,
however large, rather than skipping the row-level comparison of tables over the
command's default row limit.

A failed comparison never fails an ETL build, only differences and errors in the
reports.

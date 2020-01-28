===============================================================================
Cloud Based Access
===============================================================================

As the volume of data integrated into PUDL continues to increase, asking users
to either run the processing pipeline themselves, or to download hundreds of
gigabytes of data to do their own analyses will be become more challenging.

To address this we are working on automatically deploying each data release in
cloud computing environments that allow many users to remotely access the same
data, as well as computational resources required to work with that data. We
hope that this will minimize the technical and administrative overhead
associated with using PUDL.

This is all experimental right now, but we hope to have it up and running by
the v0.4.0 release of PUDL in Q2 of 2020.

-------------------------------------------------------------------------------
Pangeo
-------------------------------------------------------------------------------

Our focus right now is on the `Pangeo <https://pangeo.io>`__ platform, which
solves a similar problem for within the Earth science research community.
Pangeo uses a `JupyterHub <https://jupyterhub.readthedocs.io/en/stable/>`__
deployment, and includes commonly used scientific software packages and a
shared domain specific data repository, which users may access remotely via
JupyterLab.

-------------------------------------------------------------------------------
BigQuery
-------------------------------------------------------------------------------

We are also looking at making the published data packages available for live
querying by inserting them into Google's
`BigQuery data warehouse <https://cloud.google.com/bigquery/>`__.

-------------------------------------------------------------------------------
Other Options
-------------------------------------------------------------------------------

Are there other cloud platforms we should consider? Feel free to
`create an issue on Github <https://github.com/catalyst-cooperative/pudl/issues>`__ and let us know!

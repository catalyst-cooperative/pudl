===============================================================================
Infrastructure as Code
===============================================================================

-------------------------------------------------------------------------------
Overview
-------------------------------------------------------------------------------

We use `terraform <https://developer.hashicorp.com/terraform>`__ to manage some
of our infrastructure. It lets us repeat infrastructure setup tasks without
having to rely on individual developers clicking the right buttons in the right
order.

-------------------------------------------------------------------------------
Setup
-------------------------------------------------------------------------------

1. Install terraform with the `official docs
   <https://developer.hashicorp.com/terraform/downloads>`__

2. Make sure you're authenticated to GCP for `*application* usage
   <https://cloud.google.com/docs/authentication/application-default-credentials>`__,
   not just normal gcloud usage: ``gcloud auth application-default login``

-------------------------------------------------------------------------------
Development
-------------------------------------------------------------------------------

Just put things into ``main.tf`` for now. You might want to check out the `GCP
tutorial
<https://developer.hashicorp.com/terraform/tutorials/gcp-get-started>`__ or the
`GCP provider docs
<https://registry.terraform.io/providers/hashicorp/google/latest/docs>`__.

Run:

1. ``tf init`` so you get the GCP provider. You only need to do this the first time.
2. ``tf plan`` to see what is going to happen.
3. ``tf apply`` to make changes to infrastructure.

===============================================================================
Run a Versioned Release
===============================================================================

So the time has come and we would like to release a ``stable`` version of our
data/code out into the world. By the end of this, we'd like for:

* The release notes to have a fully dated release and a narrative overview of
  what's changed since the last release.
* The git tag ``vYYYY.M.x`` (year, month, patch) to identify a specific version.
* The ``stable`` git branch to refer to the same commit as that tag.
* Our data outputs corresponding to that tag on Zenodo,
  ``pudl.catalyst.coop/stable`` buckets in GCS/S3.
* An archive of the PUDL GitHub repository for the release on Zenodo.
* A GitHub Release created for that tag with installation instructions.
* Updated ``stable`` and ``vYYYY.M.x`` documentation on ReadTheDocs.

Here's how to do it!
--------------------

1. Tell the rest of the team you're planning on releasing a new version of the
   code in #team. Create a
   `Versioned Release Checklist
   <https://github.com/catalyst-cooperative/pudl/issues/new/choose>`__ issue to
   track your progress.

3. Create a new branch off of ``main`` to run the build and deployment from.

2. Update the release notes in ``docs/release_notes.rst``: Add a
   paragraph or two explaining what this new release is, and attach a specific
   date to the release.
   Review `PRs merged since the last release
   <https://github.com/catalyst-cooperative/pudl/pulls?q=is%3Apr+is%3Amerged+merged%3A%3E2025-12-15>`__,
   and ensure they're all listed in the notes.
   Populate the top of the release notes with a blank
   section for the subsequent release.

3. Run a branch build from your new branch by kicking off the ``build-pudl`` workflow.

4. Once everything is passing and the release notes update is in,
   push a git tag ``vYYYY.M.x`` that points at the head of your branch.Pushing the
   tag will trigger the ``deploy-pudl`` workflow.

5. Verify that a new release with that tag has appeared on GitHub.

6. Verify that a new archive of the PUDL repository has appeared on Zenodo.

7. Once the ``deploy-pudl`` action is complete, verify the following:

   * GCS/AWS distribution buckets have the appropriate data
   * ``stable`` and ``vYYYY.M.x`` point at the same Git ref
   * ReadTheDocs for ``stable`` and ``vYYYY.M.x`` versions have the latest
     changes in the release notes
   * GitHub Releases has the new version with appropriate release notes.

8. Verify that the Zenodo draft deposition has all the expected data (raw FERC
   databases, PUDL database, everything the right size. Compare to the files in
   the GCS/S3 distribution buckets). If the ``zenodo-data-release`` action failed,
   you can re-run it manually with settings:

   * Zenodo server environment: ``production``
   * Path to publish: path for the release tag
   * Ignore regex: default
   * Automatically publish: ``no-publish``

   If Zenodo is extra cranky, upload the files to the draft deposition manually.

9. Update the Zenodo metadata to include the description from the release notes and
   update the links to other resources related to the release. Make sure all the
   metadata fields are complete and accurate.

10. Publish the Zenodo deposition! Wahoo! You're now done!

11. Tell #team that the release is complete, and they can resume merges to ``main``.
    Remind folks that release notes in open PRs may need to be adjusted to make sure
    the notes are filed under the correct release number.

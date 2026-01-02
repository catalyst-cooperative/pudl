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
   code in #team. Remind them that any changes to the release notes *after* the
   next nightly build has passed need to go into a new version. Create a
   `Versioned Release Checklist
   <https://github.com/catalyst-cooperative/pudl/issues/new/choose>`__ issue to
   track your progress.

2. The night before you plan to release, update the release notes with a
   paragraph or two explaining what this new release is, and attach a specific
   date to the release.

3. Wait for the nightly build to pass with your release notes updates.

4. If the nightly build passes, push a git tag ``vYYYY.M.x`` that points at the commit
   which just passed the nightly build. This will kick off another build and
   create a GitHub Release.

5. Verify that a new release with that tag has appeared on GitHub.

6. Verify that a new archive of the PUDL repository has appeared on Zenodo.

7. If *that* build passes, verify the following:

   * GCS/AWS distribution buckets have the appropriate data
   * ``stable`` and ``vYYYY.M.x`` point at the same Git ref
   * ReadTheDocs for ``stable`` and ``vYYYY.M.x`` versions have the latest
     changes in the release notes
   * GitHub Releases has the new version with appropriate release notes.

8. Verify that the Zenodo draft deposition has all the expected data (raw FERC
   databases, PUDL database, everything the right size. Compare to the files in
   the GCS/S3 distribution buckets).

9. Update the Zenodo metadata to include the description from the release notes and
   update the links to other resources related to the release. Make sure all the
   metadata fields are complete and accurate.

10. Publish the Zenodo deposition! Wahoo! You're now done!

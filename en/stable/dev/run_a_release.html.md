# Run a Versioned Release

So the time has come and we would like to release a `stable` version of our
data/code out into the world. By the end of this, we’d like for:

* The release notes to have a fully dated release and a narrative overview of
  what’s changed since the last release.
* The git tag `vYYYY.M.x` (year, month, patch) to identify a specific version.
* The `stable` git branch to refer to the same commit as that tag.
* Our data outputs corresponding to that tag on Zenodo,
  `pudl.catalyst.coop/stable` buckets in GCS/S3.
* An archive of the PUDL GitHub repository for the release on Zenodo.
* A GitHub Release created for that tag with installation instructions.
* Updated `stable` and `vYYYY.M.x` documentation on GitHub Pages.

## Here’s how to do it!

1. Tell the rest of the team you’re planning on releasing a new version of the
   code in #team. Hold all merges to `main` not associated with the release
   until after the release is complete. Create a
   [Versioned Release Checklist](https://github.com/catalyst-cooperative/pudl/issues/new/choose) issue to
   track your progress.
2. Update the release notes in `docs/release_notes.rst`: Add a
   paragraph or two explaining what this new release is, and attach a specific
   date to the release.
   Review [PRs merged since the last release](https://github.com/catalyst-cooperative/pudl/pulls?q=is%3Apr+is%3Amerged+merged%3A%3E2025-12-15),
   and ensure they’re all listed in the notes.
   Populate the top of the release notes with a blank
   section for the subsequent release.
3. Check that `.zenodo.json` lists the current Catalyst membership and an
   up-to-date set of keywords – the `zenodo-data-release` action reads creators
   and keywords directly from this file, so it needs to be current *before* the
   release tag is pushed, not fixed up after the fact.
4. Once the release notes and `.zenodo.json` updates are in, push a git tag
   `vYYYY.M.x` that points at the head of `main`. Pushing the tag will trigger
   the `build-pudl` workflow. If there is already a successful build from the
   tagged commit, then the build will be skipped and it will immediately trigger a
   deployment.
5. Verify that a new release with that tag has appeared on GitHub.
6. Verify that a new archive of the PUDL repository has appeared on Zenodo.
7. Once the `deploy-pudl` action is complete, verify the following:
   * GCS/AWS distribution buckets have the appropriate data
   * `stable` and `vYYYY.M.x` point at the same Git ref
   * GitHub Pages for `stable` and `vYYYY.M.x` versions have the latest
     changes in the release notes
   * GitHub Releases has the new version with appropriate release notes.
8. Verify that the Zenodo draft deposition has all the expected data (raw FERC
   databases, PUDL database, everything the right size. Compare to the files in
   the GCS/S3 distribution buckets). If the `zenodo-data-release` action failed,
   you can re-run it manually with settings:
   * Zenodo server environment: `production`
   * Path to publish: path for the release tag
   * Ignore regex: default
   * PUDL version: `vYYYY.M.x`
   * Metadata only: unchecked
   * Automatically publish: `no-publish`

   If Zenodo is extra cranky, upload the files to the draft deposition manually.
9. Verify the Zenodo deposition’s metadata (creators, keywords, version, license,
   description, and related resource links) looks right. The `zenodo-data-release`
   action populates all of this automatically from `.zenodo.json`,
   `pudl.metadata.sources`, and the release notes for this version – it doesn’t
   need to be edited by hand. If something’s off (e.g. a typo slipped into the
   release notes after this ran, or someone was added to `.zenodo.json` late), fix
   the underlying source and re-run the metadata step without re-uploading the data:
   ```console
   pixi run zenodo_data_release \
     --env production \
     --pudl-version vYYYY.M.x \
     --metadata-only \
     --no-publish
   ```

   This must be run from a checkout of the `vYYYY.M.x` tag itself (`git checkout
   vYYYY.M.x` first) – creators, keywords, and the release notes text are all read
   live from the working tree, and the script refuses to run against production from
   a mismatched checkout.
10. Publish the Zenodo deposition! Wahoo! You’re now done!
11. Tell #team that the release is complete, and they can resume merges to `main`.
    Remind folks that release notes in open PRs may need to be adjusted to make sure
    the notes are filed under the correct release number.

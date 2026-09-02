# Releasing mbx-cache

Releases are managed by release-plz. It derives versions from crates.io and
updates `Cargo.toml` and `CHANGELOG.md` in a release PR. Every commit on `main`
publishes a container image. Merging a release PR additionally publishes the
crate, Git tag, and GitHub release.

## Repository setup

Create a `RELEASE_PLZ_TOKEN` Actions secret containing a fine-grained GitHub
token for this repository with read/write access to contents and pull requests.
A token distinct from `GITHUB_TOKEN` is required so release-plz's pull requests
trigger the normal CI and review workflows.

Configure a crates.io trusted publisher for owner `jdx`, repository
`mbx-cache`, and workflow `release-plz.yml`. The release job requests a
short-lived crates.io token with GitHub OIDC; no long-lived crates.io token is
stored in GitHub.

## Normal release flow

1. Every push to `main` builds AMD64 and ARM64 images on native GitHub-hosted
   runners, combines them into a multi-platform image tagged with the full
   commit SHA (`sha-<commit>`), and moves `main` to that image. Architecture-
   specific caches keep subsequent builds fast. If builds overlap, only the
   current head commit is allowed to move `main`.
2. A push to `main` also opens or updates the release-plz release PR.
3. The daily release job enables auto-merge when the previous release is at
   least seven days old and a `feat` or `fix` commit is pending. The first
   release is allowed immediately. Manually dispatch `auto-merge-release` to
   bypass the cadence checks.
4. Merging the release PR publishes the crate to crates.io and creates a
   `vX.Y.Z` tag and draft GitHub release.
5. The same image build also adds the `X.Y.Z` and `X.Y` tags, uploads its
   immutable digest reference as `container-image.txt`, and publishes the
   release. A second container build is not run for the release.

The image reference in `container-image.txt` is the value to use for
`MBX_CACHE_IMAGE` in the Azure deployment.

## Recovery

If image publishing fails after a tag exists, manually dispatch the `release`
workflow with that tag. It rebuilds the image, replaces `container-image.txt`,
and publishes the release only after the image succeeds. Recovery does not move
the `main` image tag.

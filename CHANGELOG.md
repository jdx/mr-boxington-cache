# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.1](https://github.com/jdx/mise-cache/compare/v0.1.0...v0.1.1) - 2026-08-10

### Added

- *(protocol)* add mutable action manifests ([#14](https://github.com/jdx/mise-cache/pull/14))
- *(protocol)* add rustc action contract ([#12](https://github.com/jdx/mise-cache/pull/12))

### Fixed

- *(database)* use unique action manifest migration version ([#18](https://github.com/jdx/mise-cache/pull/18))
- *(protocol)* align action cache v1 contract ([#10](https://github.com/jdx/mise-cache/pull/10))

## [0.1.0](https://github.com/jdx/mise-cache/releases/tag/v0.1.0) - 2026-08-07

### Added

- *(deploy)* add OVH US production configuration ([#2](https://github.com/jdx/mise-cache/pull/2))
- *(auth)* add OIDC bearer authorization ([#1](https://github.com/jdx/mise-cache/pull/1))
- add self-hosted mise cache service

### Fixed

- *(release)* harden and streamline release CI ([#5](https://github.com/jdx/mise-cache/pull/5))

### Other

- *(release)* automate container releases ([#3](https://github.com/jdx/mise-cache/pull/3))
- *(ci)* update docker actions
- *(ci)* cache container build layers
- *(ci)* update checkout action

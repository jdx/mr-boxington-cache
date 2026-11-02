# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.0.1](https://github.com/jdx/mr-boxington-cache/compare/v0.0.0...v0.0.1) - 2026-11-02

### Added

- expose database pool connections on /metrics ([#115](https://github.com/jdx/mr-boxington-cache/pull/115))
- make the database acquire timeout configurable ([#114](https://github.com/jdx/mr-boxington-cache/pull/114))
- *(storage)* add a Google Cloud Storage backend ([#102](https://github.com/jdx/mr-boxington-cache/pull/102))
- *(chart)* add deployment extension points and fix the pod uid ([#103](https://github.com/jdx/mr-boxington-cache/pull/103))
- *(auth)* support scoped anonymous cache reads ([#56](https://github.com/jdx/mr-boxington-cache/pull/56))
- answer batched action lookups and packed uploads ([#47](https://github.com/jdx/mr-boxington-cache/pull/47))
- *(server)* compress transfers with zstd when the client asks ([#37](https://github.com/jdx/mr-boxington-cache/pull/37))
- *(metadata)* sweep the rows expired objects leave behind ([#35](https://github.com/jdx/mr-boxington-cache/pull/35))
- *(metadata)* record when blobs are served ([#29](https://github.com/jdx/mr-boxington-cache/pull/29))
- *(protocol)* expose blob pack response metadata ([#25](https://github.com/jdx/mr-boxington-cache/pull/25))
- *(metrics)* instrument blob pack transfers ([#23](https://github.com/jdx/mr-boxington-cache/pull/23))
- *(protocol)* add mutable action manifests ([#14](https://github.com/jdx/mr-boxington-cache/pull/14))
- *(protocol)* add rustc action contract ([#12](https://github.com/jdx/mr-boxington-cache/pull/12))
- *(deploy)* add OVH US production configuration ([#2](https://github.com/jdx/mr-boxington-cache/pull/2))
- *(auth)* add OIDC bearer authorization ([#1](https://github.com/jdx/mr-boxington-cache/pull/1))
- add self-hosted mise cache service

### Fixed

- drop uploads whose body goes idle ([#118](https://github.com/jdx/mr-boxington-cache/pull/118))
- stop Azure cutting blob streams after 30 seconds ([#116](https://github.com/jdx/mr-boxington-cache/pull/116))
- answer database pool exhaustion with 503 and Retry-After ([#113](https://github.com/jdx/mr-boxington-cache/pull/113))
- accept the action descriptors current clients send ([#110](https://github.com/jdx/mr-boxington-cache/pull/110))
- *(deps)* update webpki crates past the emSign advisories ([#95](https://github.com/jdx/mr-boxington-cache/pull/95))
- *(deps)* update rust crates ([#72](https://github.com/jdx/mr-boxington-cache/pull/72))
- *(deps)* update sqlx to 0.9 and rename runtime and TLS features ([#78](https://github.com/jdx/mr-boxington-cache/pull/78))
- *(deps)* update rust crate jsonwebtoken to v11 ([#77](https://github.com/jdx/mr-boxington-cache/pull/77))
- update yanked chacha20 dependency ([#59](https://github.com/jdx/mr-boxington-cache/pull/59))
- accept complete cache seed data ([#58](https://github.com/jdx/mr-boxington-cache/pull/58))
- accept cc and build-script cache actions ([#57](https://github.com/jdx/mr-boxington-cache/pull/57))
- accept rustc linker identities ([#52](https://github.com/jdx/mr-boxington-cache/pull/52))
- fix action batch metadata latency ([#49](https://github.com/jdx/mr-boxington-cache/pull/49))
- *(deploy)* unbreak the production deploy after the rebrand ([#27](https://github.com/jdx/mr-boxington-cache/pull/27))
- *(database)* use unique action manifest migration version ([#18](https://github.com/jdx/mr-boxington-cache/pull/18))
- *(protocol)* align action cache v1 contract ([#10](https://github.com/jdx/mr-boxington-cache/pull/10))
- *(release)* harden and streamline release CI ([#5](https://github.com/jdx/mr-boxington-cache/pull/5))

### Other

- reset the crate version to 0.0.0 so release-plz opens a release PR
- record blob access off the read path ([#112](https://github.com/jdx/mr-boxington-cache/pull/112))
- check an output directory's files in one query ([#117](https://github.com/jdx/mr-boxington-cache/pull/117))
- *(deps)* lock file maintenance ([#111](https://github.com/jdx/mr-boxington-cache/pull/111))
- store packed blobs concurrently and skip GCS staging for small blobs ([#109](https://github.com/jdx/mr-boxington-cache/pull/109))
- *(deps)* lock file maintenance ([#99](https://github.com/jdx/mr-boxington-cache/pull/99))
- *(deps)* lock file maintenance ([#89](https://github.com/jdx/mr-boxington-cache/pull/89))
- *(deps)* update rustls to 0.23.46 ([#88](https://github.com/jdx/mr-boxington-cache/pull/88))
- *(deps)* lock file maintenance ([#82](https://github.com/jdx/mr-boxington-cache/pull/82))
- reduce development debug info ([#69](https://github.com/jdx/mr-boxington-cache/pull/69))
- *(deps)* lock file maintenance ([#68](https://github.com/jdx/mr-boxington-cache/pull/68))
- fix the GitHub Action docs link ([#66](https://github.com/jdx/mr-boxington-cache/pull/66))
- skip blob pack size preflight ([#60](https://github.com/jdx/mr-boxington-cache/pull/60))
- Move the production cache to Azure Blob Storage ([#53](https://github.com/jdx/mr-boxington-cache/pull/53))
- disclose current CI performance ([#51](https://github.com/jdx/mr-boxington-cache/pull/51))
- reject empty database connection pools ([#50](https://github.com/jdx/mr-boxington-cache/pull/50))
- link the user documentation ([#42](https://github.com/jdx/mr-boxington-cache/pull/42))
- refresh the README ([#41](https://github.com/jdx/mr-boxington-cache/pull/41))
- *(protocol)* use shared wire contract ([#39](https://github.com/jdx/mr-boxington-cache/pull/39))
- *(metadata)* cover the PostgreSQL store in CI ([#30](https://github.com/jdx/mr-boxington-cache/pull/30))
- [**breaking**] rebrand to mbx-cache ([#26](https://github.com/jdx/mr-boxington-cache/pull/26))
- *(protocol)* stream blob packs ([#21](https://github.com/jdx/mr-boxington-cache/pull/21))
- release v0.1.1 ([#11](https://github.com/jdx/mr-boxington-cache/pull/11))
- *(release)* publish crates.io releases ([#7](https://github.com/jdx/mr-boxington-cache/pull/7))
- release v0.1.0 ([#4](https://github.com/jdx/mr-boxington-cache/pull/4))
- *(release)* automate container releases ([#3](https://github.com/jdx/mr-boxington-cache/pull/3))
- *(ci)* update docker actions
- *(ci)* cache container build layers
- *(ci)* update checkout action

### Security

- update rustls past the TLS 1.3 handshake advisory ([#61](https://github.com/jdx/mr-boxington-cache/pull/61))
- restrict OIDC writes and harden tree validation ([#44](https://github.com/jdx/mr-boxington-cache/pull/44))

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

# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.1](https://github.com/Nehliin/vortex/compare/vortex-cli-v0.3.0...vortex-cli-v0.3.1) - 2026-02-08

### Changed 

- Removed the dimmed ui during the downloading metadata stage ([#100](https://github.com/Nehliin/vortex/pull/100))

### Fixed 

- Properly report peers, upload/download throughput during the downloading metadata state ([#100](https://github.com/Nehliin/vortex/pull/100))

### Added

- Report metadata download progress in the downloading state ([#100](https://github.com/Nehliin/vortex/pull/100))
- Improve CLI error reporting by moving over to color_eyre ([#98](https://github.com/Nehliin/vortex/pull/98))
- Support skipping use of dht node cache using the `--skip-dht-cache` ([#97](https://github.com/Nehliin/vortex/pull/97))

## [0.3.0](https://github.com/Nehliin/vortex/compare/vortex-cli-v0.2.0...vortex-cli-v0.3.0) - 2026-01-25

- (refactor) Make metrics an optional feature to vortex-cli ([#93](https://github.com/Nehliin/vortex/pull/93))
- (fix) Crash fixes and announce on the DHT more frequently ([#90](https://github.com/Nehliin/vortex/pull/90))
- (feat) Add XDG directory support to the cli ([#87](https://github.com/Nehliin/vortex/pull/87))
- (feat) Support port selection and track total download time in cli ([#86](https://github.com/Nehliin/vortex/pull/86))
- (style) Update UI and ensure app continues seeding after completion ([#85](https://github.com/Nehliin/vortex/pull/85))

## [0.2.0](https://github.com/Nehliin/vortex/compare/vortex-cli-v0.1.0...vortex-cli-v0.2.0) - 2026-01-15

- Update cli to better match new state changes (#49)
- Use jemalloc and fix LinkTimeout UB (#50)
- Ratatui UI for CLI (#52)
- Track upload speed in `PeerMetrics` and support resuming from partially downloaded content (#58)

## [0.1.0] - 2025-02-22

- Initial version

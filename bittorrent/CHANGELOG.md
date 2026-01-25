# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.0](https://github.com/Nehliin/vortex/compare/vortex-bittorrent-v0.3.0...vortex-bittorrent-v0.4.0) - 2026-01-25

- (fix) ECONNRESET during TCP handshakes weren't properly handled and invalid bittorrent handshakes with the correct length would cause panics ([#90](https://github.com/Nehliin/vortex/pull/90))
- (perf) Make the release profile more aggressive + migrate more asserts to debug_asserts ([#88](https://github.com/Nehliin/vortex/pull/88))
- (feat) Support serde serialization for the config ([#87](https://github.com/Nehliin/vortex/pull/87))
- Remove num_unchoked, endgame and snubbed from TorrentEvents  ([#86](https://github.com/Nehliin/vortex/pull/86))
- (fix) Crash when peer disconnected before write completed ([#85](https://github.com/Nehliin/vortex/pull/85))
- (feat) Metrics improvements ([#84](https://github.com/Nehliin/vortex/pull/84))

## [0.3.0](https://github.com/Nehliin/vortex/compare/vortex-bittorrent-v0.2.0...vortex-bittorrent-v0.3.0) - 2026-01-15

- Ensure being choked makes queued pieces available to other peers (#34)
- Improve our intrest reporting to peers  (#36)
- Send `Have` messages when pieces complete + more checks +  tests (#37)
- Add metrics integration (#38)
- Update Reject piece logic and add more tests (#39)
- Revamp timeouts (#40)
- Generalize peer_provider to accept more commands to the event loop + timeout tests (#43)
- Implement max connections cap + panic fixes (#44)
- Improve connection shutdown flow and tweak max target inflight (#45)
- Add Endgame mode to peers (#47)
- Implement BEP 09 & BEP 10 (#48)
- Add TorrentEvents to make it easier for clients to read updates (#51)
- Connection shutdown cleanup (#53, #54)
- Simplify peer metrics reporting + feature gate metrics (#57)
- Track upload speed in `PeerMetrics` and support resuming from partially downloaded content (#58)
- Implement proper unchoking algorithm (#60)
- Support "upload_only" BEP 21 extension (#62)
- Implement round robin unchoking strategy when seeding (#63)
- Start making a public api + actual way to modify configuration (#64, #66)
- Generate peer id prefix based off crate version (#70)
- Write more messages per buffer (#73)
- Rewrite buffer pool implementation (#74, #75)
- Update deps (#78)
- (Yet another) Disk I/O rewrite to use proper async io_uring operations (#77)

### Bug fixes 

- Fix UB in connection code (#41)
- Fix bitfield bugs and return to u8 store backing (#42)
- Fix panic with last_received_subpiece (#46)
- Use jemalloc and fix LinkTimeout UB (#50)
- Avoid panicking unnecessarily (#55)
- Fix accept connection flow (#56)
- Track that buffers are returned to the pool and fix pool leak (#76)
- Fix interleaved writes under high load due to partial writes (#81)

### 💼 Other

- Move stale projects to separate repositories (#82)


## [0.2.0] - 2025-02-22

- Move over to raw-uring instead of tokio-uring (#18)
- Support FAST_EXT (#19, #20)
- Improved mmapped disk-io (#26)
- Bump to rust 2024 (#29)

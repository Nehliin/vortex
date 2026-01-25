# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Architecture

Vortex is a high-performance BitTorrent client built in Rust using io-uring for asynchronous I/O. The project is structured as a Cargo workspace with four main components:

- **`cli/`**: Terminal UI application using Ratatui for downloading torrents with DHT support. This is the primary client crate used to test that the `bittorrent` crate can be used to create meaingful applications.
- **`bittorrent/`**: Core BitTorrent protocol implementation built on io-uring. 

The bittorrent crate uses an event-driven architecture with io-uring for high-performance networking. Key modules include:
- `event_loop`: Central event dispatcher managing connections and I/O
- `peer_comm`: Peer connection management and protocol handling
- `peer_comm/tests.rs`: Peer protocol tests which require no prior setup (like podman containers)
- `piece_selector`: Download strategy and piece management
- `file_store`: Disk I/O operations for torrent data

## Development Commands

### Building and Testing
```bash
# Build all workspace members
cargo build

# Run the unit tests for the protocol logic
cargo nextest run peer_comm

# Run all tests including integration tests (requires Docker/Podman for Transmission containers)
just test

# Run specific integration test locally
just test-locally

# Format code
cargo fmt

# Lint code
cargo clippy --all-features --all-targets -- -D warnings

# Check dependencies for security issues and licensing
cargo deny check
```

### Integration Test Environment Setup
Tests use Transmission containers for seeding torrents. The `just test` command automatically:
1. Generates test files if needed
2. Sets up Transmission seed containers via `scripts/transmission_containers.sh`
3. Runs tests with `cargo nextest`

### Monitoring
Set up Grafana dashboard for metrics:
```bash
just setup-grafana
```

## Key Dependencies and Constraints

- **io-uring**: Linux-specific async I/O interface (requires Linux)
- **cargo-deny**: Enforces dependency policies (see `deny.toml`)
- **jemalloc**: Used as global allocator in CLI
- **Edition 2024**: All crates use Rust edition 2024

Banned dependencies include OpenSSL, async-std, and deprecated crates as specified in `deny.toml`.

## CLI Usage
The CLI (`vortex-cli`) provides a terminal interface for downloading torrents with real-time progress visualization and DHT peer discovery.

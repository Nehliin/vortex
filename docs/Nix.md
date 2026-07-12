# Nix Development Environment

Vortex ships with a [Nix flake](../flake.nix) that provides a reproducible
development environment. Every contributor gets the same Rust toolchain and
the same versions of `cargo-nextest`, `cargo-deny`, `just`, `mold`, and every
other tool used by the project when using nix.

The flake is Linux-only (`x86_64-linux`, `aarch64-linux`) because Vortex is
built on io-uring.

## Prerequisites

Install Nix with flake support enabled.

**Using the [official installer](https://nixos.org/download/)**, followed by manually enabling flakes:

```bash
sh <(curl -L https://nixos.org/nix/install) --daemon
mkdir -p ~/.config/nix
echo 'experimental-features = nix-command flakes' >> ~/.config/nix/nix.conf
```

Verify:

```bash
nix --version           # >= 2.18
nix flake --help        # should not error
```

## Entering the dev shell

From the repository root:

```bash
nix develop
```

The first invocation downloads nixpkgs, the Rust toolchain, and every tool
listed in [`flake.nix`](../flake.nix). Subsequent invocations reuse the Nix
store and start in seconds.

You will see:

```
Vortex Rust Dev Shell Loaded (nix develop)
  Rust:      rustc 1.xx.x (…)
  Cargo:     cargo 1.xx.x (…)
  Just:      just x.y.z
```

Everything inside the shell — `cargo`, `rustc`, `just`, `mold`, etc.
— comes from Nix. Your global toolchain is not used.

Exit with `exit` or `Ctrl-D`.

### `nix-shell` compatibility

If you (or an editor plugin) still call the legacy `nix-shell`, a
[`shell.nix`](../shell.nix) shim delegates to the same flake devShell:

```bash
nix-shell
```

## What is provided

The dev shell includes:

| Tool | Purpose |
|------|---------|
| Rust toolchain | Pinned via [`rust-toolchain.toml`](../rust-toolchain.toml). Includes `rustfmt`, `clippy`, `rust-src`, `llvm-tools`. |
| `cargo-nextest` | Test runner used by `just test`. |
| `cargo-deny` | License and advisory checks (see [`deny.toml`](../deny.toml)). |
| `cargo-audit` | Vulnerability scan of `Cargo.lock`. |
| `cargo-machete` | Detects unused workspace dependencies. |
| `cargo-llvm-cov` | Coverage reports. |
| `cargo-fuzz` | Drives the fuzz targets in [`fuzz/`](../fuzz/). |
| `just` | Runs the [`justfile`](../justfile) recipes. |
| `git-cliff` | Changelog generation (see [`cliff.toml`](../cliff.toml)). |
| `mold` | Fast linker on Linux. |
| `pkg-config` | For crates that link native libraries. |
| `nil`, `nixfmt-rfc-style` | Nix LSP and formatter. |

## Building a release binary with Nix

The flake exposes a `packages.default` output that builds `vortex-cli`
hermetically from `Cargo.lock`:

```bash
nix build              # produces ./result/bin/vortex-cli
./result/bin/vortex-cli --help
```

This does **not** require entering the dev shell — Nix builds it in a
sandbox using the pinned toolchain.

## Updating the pinned inputs

`flake.lock` pins `nixpkgs`, `rust-overlay`, and `flake-utils` to specific
commits. To pull newer versions:

```bash
nix flake update                       # update all inputs
nix flake update --update-input nixpkgs   # update just one
```

Commit the updated `flake.lock` so every contributor gets the same versions.

## Troubleshooting

**`error: Path 'flake.nix' … is not tracked by Git.`**
Nix flakes only see files known to Git. After creating a new file, run
`git add -N <file>` (or a real `git add`) before `nix develop`.

**`error: experimental Nix feature 'flakes' is disabled`**
Enable flakes — see the prerequisites section.

**`error: attribute 'x86_64-darwin' missing`**
The flake is Linux-only. Vortex depends on io-uring, which does not exist on
macOS.

**Slow first build.**
The initial `nix develop` downloads the Rust toolchain and every dev tool
(hundreds of MB). This is a one-time cost per `flake.lock` revision.

**Global `cargo` picking wrong toolchain outside Nix.**
The [`rust-toolchain.toml`](../rust-toolchain.toml) file makes `rustup`
select the same channel outside the dev shell, so both environments stay
aligned.

# When adding new tools to this flake, always check the official registry to
# find the correct attribute name:
# * Search Tool: NixOS Package Search — https://search.nixos.org/packages
# * Usage: If you find ripgrep, simply add pkgs.ripgrep to the buildInputs.
{
  description = "Vortex — high-performance BitTorrent client built on io-uring";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay.url = "github:oxalica/rust-overlay";
  };

  outputs = {
    self,
    nixpkgs,
    flake-utils,
    rust-overlay,
  }:
  # io-uring is Linux-only, so restrict to Linux systems.
    flake-utils.lib.eachSystem ["x86_64-linux" "aarch64-linux"] (
      system: let
        overlays = [(import rust-overlay)];
        pkgs = import nixpkgs {inherit system overlays;};

        # Define the rust toolchain from rust-toolchain.toml
        rustToolchain = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;

        # Read the CLI crate metadata (the workspace root Cargo.toml has no [package]).
        cliCargoToml = builtins.fromTOML (builtins.readFile ./cli/Cargo.toml);
      in {
        # `nix build` and `nix run` — builds the CLI release binary from the workspace.
        packages.default = pkgs.rustPlatform.buildRustPackage {
          pname = cliCargoToml.package.name;
          version = cliCargoToml.package.version;
          src = ./.;
          cargoLock.lockFile = ./Cargo.lock;
          # Build only the CLI binary from the workspace.
          cargoBuildFlags = ["-p" cliCargoToml.package.name];
          cargoTestFlags = ["-p" cliCargoToml.package.name];
        };

        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
            rustToolchain
            cargo-nextest
            cargo-machete
            cargo-audit
            cargo-deny
            cargo-llvm-cov # Coverage instrumentation via LLVM
            cargo-fuzz # Fuzz targets live in ./fuzz
            just # Command runner
            git-cliff # Changelog generator (see cliff.toml)
            mold # Fast linker
            pkg-config # For crates that link native libraries
            nixfmt-rfc-style # Nix formatter (RFC 166)
            nil # Nix language server
          ];

          shellHook = ''
            echo "Vortex Rust Dev Shell Loaded (nix develop)"
            echo "  Rust:      $(rustc --version)"
            echo "  Cargo:     $(cargo --version)"
            echo "  Just:      $(just --version 2>/dev/null || echo n/a)"
            echo ""
            echo "Tip: run 'just' to list project commands."
          '';
        };
      }
    );
}

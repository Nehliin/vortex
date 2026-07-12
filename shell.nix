# Compatibility shim for `nix-shell` users.
# Delegates to the flake devShell so both entry points get identical environments.
# Prefer `nix develop` (or direnv with `use flake`) for the full experience.
let
  flake = builtins.getFlake (toString ./.);
  system = builtins.currentSystem;
in
  flake.devShells.${system}.default

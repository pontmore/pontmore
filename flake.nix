{
  description = "Pontmore reference implementation";

  inputs = {
    nixpkgs = {
      url = "github:nixos/nixpkgs/nixos-26.05";
    };
    nixpkgs-unstable = {
      url = "github:nixos/nixpkgs/nixos-unstable";
    };
    flake-utils.url = "github:numtide/flake-utils";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flakebox = {
      url = "github:rustshop/flakebox?rev=b36f6b7cb77652c1d4cb7279d8f47fddf07066ae";
      inputs.nixpkgs.follows = "nixpkgs-unstable";
      inputs.fenix.follows = "fenix";
    };
    # flakebox links with wild and `--compress-debug-sections=zstd`, which needs a
    # newer wild than nixpkgs ships.
    wild = {
      url = "github:davidlattimore/wild/0.9.0";
      inputs.nixpkgs.follows = "nixpkgs-unstable";
    };
    advisory-db = {
      url = "github:rustsec/advisory-db";
      flake = false;
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      nixpkgs-unstable,
      flake-utils,
      fenix,
      flakebox,
      advisory-db,
      ...
    }@inputs:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [
            (
              final: prev:
              let
                unstableWildPkgs = import nixpkgs-unstable {
                  inherit system;
                  overlays = [ (import inputs.wild) ];
                };
              in
              {
                inherit (unstableWildPkgs) wild wild-unwrapped;
              }
            )
          ];
        };
        lib = pkgs.lib;

        # `rust-toolchain.toml` stays the single source of truth for the Rust
        # version, so rustup users and Nix users build with the same compiler. Fenix
        # only exposes a release date, so `checks.toolchainVersion` compares the
        # locked stable compiler against it.
        rustToolchainVersion = (lib.importTOML ./rust-toolchain.toml).toolchain.channel;
        fenixStable = fenix.packages.${system}.stable;

        flakeboxLib = flakebox.lib.mkLib pkgs {
          config = {
            toolchain.channel = "stable";
            toolchain.components = [
              "rustc"
              "cargo"
              "clippy"
              "rust-analyzer"
              "rust-src"
            ];
            # Format with the pinned stable rustfmt, like CI, instead of nightly.
            toolchain.rustfmt = fenixStable.rustfmt;

            # The repository keeps its hand-written justfile, rustfmt.toml, .envrc,
            # and CI workflow.
            just.enable = false;
            rust.rustfmt.enable = false;
            direnv.enable = false;
            github.ci.enable = false;

            # Checks that do not match this repository's conventions or config.
            convco.enable = false;
            semgrep.enable = false;
            typos.enable = false;
            treefmt.enable = false;
            ast-grep.enable = false;
            cargo-crap.enable = false;

            git.commit-msg.enable = false;
            git.commit-template.enable = false;
            git.pre-commit.hooks = {
              nixfmt = ''
                git ls-files -z '*.nix' | xargs -0 --no-run-if-empty nixfmt --check
              '';
              prettier = ''
                npm run --silent format:files:check
              '';
            };
          };
        };

        toolchainNative = flakeboxLib.mkFenixToolchain {
          targets = lib.getAttrs [ "default" ] (flakeboxLib.mkStdTargets { });
        };

        craneMultiBuild = import nix/flakebox.nix {
          inherit
            pkgs
            flakeboxLib
            advisory-db
            ;
          toolchains = {
            default = toolchainNative;
          };
          profiles = [
            "dev"
            "ci"
            "release"
          ];
        };

        # Development-only formatter dependencies, installed from package-lock.json
        # by Nix rather than by `npm ci`.
        nodejs = pkgs.nodejs_22;
        nodeModules = pkgs.importNpmLock.buildNodeModules {
          npmRoot = ./.;
          inherit nodejs;
        };

        devShells = {
          default = flakeboxLib.mkDevShell {
            toolchain = toolchainNative;
            packages = [
              # task runner and repository tooling
              pkgs.just
              pkgs.git
              pkgs.nixfmt
              pkgs.shellcheck

              # formatters
              nodejs

              # local services and daemon smoke tests
              pkgs.postgresql_17
              pkgs.python3
              pkgs.curl
              pkgs.jq
              pkgs.openssl
              pkgs.coreutils

              # containers: local stack, image inspection, and registry publishing
              pkgs.docker-client
              pkgs.docker-compose
              pkgs.skopeo
              pkgs.manifest-tool

              # dependency auditing
              pkgs.cargo-audit
            ];

            shellHook = ''
              export REPO_ROOT="$(git rev-parse --show-toplevel)"

              # Link the Nix-built formatter packages where prettier resolves plugins.
              if [ -e "$REPO_ROOT/node_modules" ] && [ ! -L "$REPO_ROOT/node_modules" ]; then
                >&2 echo "⚠️  $REPO_ROOT/node_modules is not managed by Nix; remove it to use the locked formatters"
              else
                ln -sfn ${nodeModules}/node_modules "$REPO_ROOT/node_modules"
              fi

              export PONTMORE_TEST_DATABASE_URL="''${PONTMORE_TEST_DATABASE_URL:-postgres://pontmore@127.0.0.1:''${PONTMORE_DEV_PG_PORT:-55432}/pontmore_test}"
            '';
          };

          lint = flakeboxLib.mkLintShell {
            packages = [
              pkgs.nixfmt
              pkgs.shellcheck
              nodejs
            ];
          };
        };
      in
      {
        inherit devShells;

        # Nested per-profile outputs, e.g. `nix build .#ci.workspaceClippy`.
        legacyPackages = craneMultiBuild;

        packages = {
          default = craneMultiBuild.pontmored;
          inherit (craneMultiBuild)
            pontmored
            pontmore-tck
            pontmore-pkgs
            # RustSec audit; not a blocking check because advisories change daily.
            workspaceAudit
            ;
        };

        checks = {
          inherit (craneMultiBuild.ci)
            workspaceClippy
            workspaceTest
            tckVectors
            ;
          container = craneMultiBuild.container.pontmored;

          toolchainVersion = pkgs.runCommand "pontmore-toolchain-version" { } ''
            actual=$(${toolchainNative.toolchain}/bin/rustc --version | cut -d' ' -f2)
            if [ "$actual" != "${rustToolchainVersion}" ]; then
              echo "fenix stable is Rust $actual, but rust-toolchain.toml pins ${rustToolchainVersion}." >&2
              echo "Update the fenix lock or the toolchain file so they agree." >&2
              exit 1
            fi
            echo "$actual" > $out
          '';
        };

        formatter = pkgs.nixfmt;
      }
    );
}

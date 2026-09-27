{
  pkgs,
  flakeboxLib,
  toolchains,
  advisory-db,
  profiles,
}:
let
  lib = pkgs.lib;

  # Files needed to build only the third-party dependencies. Keeping this set small
  # lets Nix reuse the (expensive) dependency derivation across source edits.
  filterWorkspaceDepsBuildFilesRegex = [
    "Cargo\\.lock"
    "Cargo\\.toml"
    ".cargo"
    ".cargo/.*"
    "crates/[^/]+/Cargo\\.toml"
  ];

  # Files the workspace embeds at compile time, including test targets.
  filterWorkspaceBuildFilesRegex = filterWorkspaceDepsBuildFilesRegex ++ [
    ".*\\.rs"
    "crates/daemon/migrations/.*\\.sql"
    "crates/daemon/assets/.*"
    "tck/vectors/.*\\.json"
  ];

  # Files read by tests at run time.
  filterWorkspaceTestFilesRegex = filterWorkspaceBuildFilesRegex ++ [
    "deploy/.*"
    "examples/.*"
  ];

  commonSrc = builtins.path {
    path = ./..;
    name = "pontmore";
  };

  filterSrcWithRegexes =
    regexes: src:
    let
      basePath = toString src + "/";
    in
    lib.cleanSourceWith {
      filter =
        path: type:
        let
          relPath = lib.removePrefix basePath (toString path);
        in
        type == "directory" || lib.any (re: builtins.match re relPath != null) regexes;
      inherit src;
    };

  commonArgs = {
    pname = "pontmore";

    nativeBuildInputs = [
      pkgs.removeReferencesTo
    ];

    # Keep line numbers in panics and backtraces.
    dontStrip = true;

    # Keep the Rust toolchain out of the runtime closure (and the container image).
    postFixup = ''
      rust_toolchain_pkg=$(dirname "$(dirname "$(command -v rustc)")")
      find "$out" -type f -executable -exec remove-references-to -t "$rust_toolchain_pkg" '{}' +
    '';
  };

  # Tests that execute `examples/connectors/simulated-escrow`. Its digest-pinned
  # entrypoint uses the host interpreter `#!/usr/bin/python3` and the daemon clears
  # `PATH` for connectors, so they cannot run inside the Nix build sandbox. CI runs
  # them through `nix develop` instead.
  sandboxSkippedTests = [
    "simulated_connector_reuses_one_correlated_process"
    "failed_connector_is_restarted_before_the_next_exchange"
    "malformed_and_timed_out_connectors_are_contained"
  ];
in
(flakeboxLib.craneMultiBuild { inherit toolchains profiles; }) (
  craneLib':
  let
    craneLib = craneLib'.overrideArgs (
      commonArgs
      // {
        src = filterSrcWithRegexes filterWorkspaceBuildFilesRegex commonSrc;
      }
    );

    craneLibTests = craneLib.overrideArgs {
      src = filterSrcWithRegexes filterWorkspaceTestFilesRegex commonSrc;
    };
  in
  rec {
    workspaceDeps = craneLib.buildWorkspaceDepsOnly {
      src = filterSrcWithRegexes filterWorkspaceDepsBuildFilesRegex commonSrc;
    };

    workspaceBuild = craneLib.buildWorkspace {
      cargoArtifacts = workspaceDeps;
    };

    workspaceClippy = craneLib.cargoClippy {
      cargoArtifacts = workspaceDeps;
      cargoExtraArgs = "--locked --workspace";
      cargoClippyExtraArgs = "--all-targets -- --deny warnings";
      doInstallCargoArtifacts = false;
    };

    # PostgreSQL integration tests skip themselves without
    # PONTMORE_TEST_DATABASE_URL; `just test` runs them in the dev shell.
    workspaceTest = craneLibTests.buildCommand {
      pname = "pontmore-test";
      cargoArtifacts = workspaceBuild;
      cmd = ''
        cargoWithProfile test --locked --workspace -- ${
          lib.concatMapStringsSep " " (name: "--skip ${name}") sandboxSkippedTests
        }
      '';
    };

    workspaceAudit = craneLib.cargoAudit {
      inherit advisory-db;
      src = filterSrcWithRegexes filterWorkspaceDepsBuildFilesRegex commonSrc;
    };

    pontmore-pkgs = craneLib.buildPackageGroup {
      pname = "pontmore-pkgs";
      packages = [
        "pontmored"
        "pontmore-tck"
      ];
    };

    pontmored = flakeboxLib.pickBinary {
      pkg = pontmore-pkgs;
      bin = "pontmored";
    };

    pontmore-tck = flakeboxLib.pickBinary {
      pkg = pontmore-pkgs;
      bin = "pontmore-tck";
    };

    tckVectors = pkgs.runCommand "pontmore-tck-vectors" { } ''
      ${pontmore-tck}/bin/pontmore-tck ${../tck/vectors} | tee $out
    '';

    container =
      let
        uid = "7300";
        # Unprivileged runtime identity, matching the former Dockerfile.
        nss = pkgs.dockerTools.fakeNss.override {
          extraPasswdLines = [ "pontmore:x:${uid}:${uid}:pontmore:/var/empty:/bin/false" ];
          extraGroupLines = [ "pontmore:x:${uid}:" ];
        };
      in
      {
        pontmored = pkgs.dockerTools.buildLayeredImage {
          name = "pontmored";
          contents = [
            pontmored
            nss
            pkgs.cacert
          ];
          config = {
            User = "${uid}:${uid}";
            Entrypoint = [ "${pontmored}/bin/pontmored" ];
            Env = [ "SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt" ];
            ExposedPorts = {
              "7300/tcp" = { };
              "7301/tcp" = { };
            };
            Labels = {
              "org.opencontainers.image.source" = "https://github.com/pontmore/pontmore";
              "org.opencontainers.image.licenses" = "MIT";
              "org.opencontainers.image.version" = pontmored.version;
            };
          };
        };
      };
  }
)

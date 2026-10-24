{
  inputs = {
    nixpkgs.url = github:NixOS/nixpkgs;
    flake-compat.url = "https://flakehub.com/f/edolstra/flake-compat/1.tar.gz";
    flake-utils.url = "github:numtide/flake-utils";
  };
  outputs = { self, nixpkgs, flake-compat, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
        manifest = (pkgs.lib.importTOML ./Cargo.toml).package;
        orchestrator = pkgs.rustPlatform.buildRustPackage {
          pname = manifest.name;
          version = manifest.version;
          src = builtins.path { path = ./.; name = "orchestrator"; };

          nativeBuildInputs = [ pkgs.protobuf ];

          cargoLock = {
            lockFile = ./Cargo.lock;
          };
        };

        orchestrator-shell = pkgs.mkShell {
          inputsFrom = [ orchestrator ];
          packages = with pkgs; [
            clippy
            rustfmt
            protobuf
          ];
        };
      in
      {
        packages = {
          inherit orchestrator;
          default = orchestrator;
        };
        devShells = {
          inherit orchestrator-shell;
          default = orchestrator-shell;
        };
      }
    );
}

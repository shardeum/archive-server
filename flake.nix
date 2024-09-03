{
  description = "Shardus archive server";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    utils.url = "github:numtide/flake-utils";
  };

  outputs = {
    self,
    nixpkgs,
    utils,
  }: let
    appName = "shardus-archive-server";
    out =
      utils.lib.eachDefaultSystem
      (system: let
        pkgs = import nixpkgs {
          inherit system;
        };
        buildNodeJs = pkgs.callPackage "${nixpkgs}/pkgs/development/web/nodejs/nodejs.nix" {python = pkgs.python3;};
        custom-nodejs = buildNodeJs {
          enableNpm = true;
          version = "18.19.1";
          sha256 = "0wp2xyz5yqcvb6949xaqpan73rfhdc3cdfsvx7vzvzc9in64yh78";
        };

        nativeBuildInputs = with pkgs; [
          pkg-config
          custom-nodejs
        ];
        buildInputs = with pkgs; [];
      in {
        # `nix develop` or direnv
        devShell = pkgs.mkShell {
          packages =
            nativeBuildInputs
            ++ buildInputs
            ++ (with pkgs; [
              nodePackages.typescript-language-server
              nodePackages.vscode-langservers-extracted
              nodePackages.prettier
            ]);
        };
      });
  in
    out
    // {
      overlay = final: prev: {
        ${appName} = self.defaultPackage.${prev.system};
      };
    };
}

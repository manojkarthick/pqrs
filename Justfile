version := "0.1.1"

homebrew:
    wget https://github.com/manojkarthick/pqrs/releases/download/v{{version}}/pqrs-macos-amd64 -O pqrs
    tar -czf pqrs-mac.tar.gz pqrs
    shasum -a 256 pqrs-mac.tar.gz
    github-upload-asset --owner manojkarthick --repo pqrs --release-tag v{{version}} --asset-path pqrs-mac.tar.gz
    rm pqrs

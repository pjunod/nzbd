# Installing nzbd

Four ways to get a binary, in increasing order of effort. All of them end
the same way: a single `nzbd` executable with no runtime dependencies —
except the optional post-processing tools (`par2`, `unrar`, `7z`), which
nzbd invokes as subprocesses when a job needs repair or extraction.

## 1. Release binaries

Every tagged release (`v*`) publishes static tarballs with sha256 sums:

- `nzbd-<tag>-linux-x86_64-musl.tar.gz` — static musl build, runs on any
  Linux including NAS boxes and minimal containers
- `nzbd-<tag>-linux-aarch64-musl.tar.gz` — same, for ARM64
- `nzbd-<tag>-macos-aarch64.tar.gz` — Apple Silicon

```sh
tar xzf nzbd-v0.1.0-linux-x86_64-musl.tar.gz
sudo install -m 755 nzbd /usr/local/bin/
nzbd --version
```

Install the post-processing tools if you want repair/unpack:

```sh
# Debian/Ubuntu
sudo apt-get install par2 unrar-free p7zip-full
# Fedora
sudo dnf install par2cmdline p7zip p7zip-plugins
# macOS
brew install par2 p7zip
```

## 2. Docker

Images are published to GHCR on every release tag:

```sh
docker run -d --name nzbd -p 6789:6789 \
  -v /data/usenet:/data \
  -v $PWD/nzbd.toml:/etc/nzbd/nzbd.toml:ro \
  ghcr.io/pjunod/nzbd:latest
```

The image bundles `par2`, `unrar-free` and `7z`, runs unprivileged as UID
1000 under `tini`, exposes `6789`, and expects its config at
`/etc/nzbd/nzbd.toml` with `/data` as the conventional download volume.
Build locally with `docker build -t nzbd .` — the repo's
[`Dockerfile`](../Dockerfile) is a two-stage build (rust:1-bookworm →
debian:bookworm-slim).

Compose and Kubernetes deployments: see [DEPLOY.md](DEPLOY.md).

## 3. Homebrew (macOS)

A formula with a service block ships in the repo:

```sh
brew install --formula ./packaging/homebrew/nzbd.rb
brew services start nzbd     # launchd-managed daemon
```

## 4. Building from source

Requirements: **Rust 1.85+** (the workspace pins `rust-version = "1.85"`)
and a C toolchain (rusqlite builds bundled SQLite).

```sh
git clone https://github.com/pjunod/nzbd.git
cd nzbd
cargo build --release -p nzbd
target/release/nzbd --version
```

Static musl build (fully self-contained Linux binary):

```sh
rustup target add x86_64-unknown-linux-musl
sudo apt-get install musl-tools          # Debian/Ubuntu
cargo build --release -p nzbd --target x86_64-unknown-linux-musl
```

Run the test suite with `cargo test --workspace`. Tests that drive the
real `par2`/`7z` binaries self-skip (with a notice) when the tools aren't
installed, so a bare checkout still passes; install the tools above for
full coverage.

## First run

```sh
nzbd run                     # defaults: ~/downloads, API on 127.0.0.1:6789
nzbd run --config nzbd.toml  # or with a config file
nzbd run --config nzbd.toml --bind 0.0.0.0:6789   # listen on all interfaces
```

If the `--config` path doesn't exist yet, the daemon starts in **first-run
setup mode**: open the web UI and a short form (paths, news server, UI
password) writes the file and restarts the daemon with it. Prefer a file?
Copy the annotated example from [CONFIGURATION.md](CONFIGURATION.md), or
convert an existing NZBGet setup:

```sh
nzbd import-config /opt/nzbget/nzbget.conf --out nzbd.toml
# prints a report: mapped options, recognized-but-N/A options, unknowns
```

Everything else — CLI, UI, connecting Sonarr — is in [USAGE.md](USAGE.md).

# nzbd — multi-stage build: static-ish Rust binary + the PP toolchain.
#
#   docker build -t nzbd \
#     --build-arg NZBD_GIT_DESCRIBE="$(git describe --tags --always --dirty --abbrev=9)" .
#   docker run -d -p 6789:6789 \
#     -v /data/usenet:/data -v ./config:/etc/nzbd nzbd
#
# Or just `make docker-build`, which fills the argument in for you.
#
# Mount the config DIRECTORY, not the file: a file bind mount whose host
# side doesn't exist yet makes Docker create a directory in its place,
# and the first-run setup UI couldn't persist the config it writes.

FROM rust:1-bookworm AS build
# The build context deliberately excludes `.git` (see .dockerignore), so
# the binary cannot work out its own commit — it has to be told. Without
# this the daemon reports `<version>+unknown`, which is deliberately loud:
# a hundred commits once shipped as an unchanging "v0.1.0" precisely
# because a missing hash failed silently (field report 2026-07-27).
ARG NZBD_GIT_DESCRIBE
ENV NZBD_GIT_DESCRIBE=${NZBD_GIT_DESCRIBE}
WORKDIR /src
COPY . .
RUN cargo build --release -p nzbd

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      par2 unrar-free p7zip-full ca-certificates tini \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/nzbd /usr/local/bin/nzbd

# Unprivileged runtime user; /data is the conventional volume mount.
# /etc/nzbd is nzbd-writable so the first-run setup UI can create the
# config when the container starts without one.
RUN useradd -r -u 1000 -m -d /var/lib/nzbd nzbd \
 && mkdir -p /data /etc/nzbd && chown nzbd /data /etc/nzbd /var/lib/nzbd
USER nzbd

# /data is the durable volume, and this names it for the daemon. With no
# config file to read `paths.main_dir` from, an env var is the only way
# boot-time recovery can find the copy of the configuration nzbd keeps
# beside its state — which is what stops a container that lost its config
# mount from coming back up as an unconfigured first-run install.
ENV NZBD_MAIN_DIR=/data
VOLUME ["/data"]
EXPOSE 6789

ENTRYPOINT ["/usr/bin/tini", "--", "nzbd"]
CMD ["run", "--config", "/etc/nzbd/nzbd.toml", "--bind", "0.0.0.0:6789"]

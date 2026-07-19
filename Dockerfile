# nzbd — multi-stage build: static-ish Rust binary + the PP toolchain.
#
#   docker build -t nzbd .
#   docker run -d -p 6789:6789 \
#     -v /data/usenet:/data -v ./config:/etc/nzbd nzbd
#
# Mount the config DIRECTORY, not the file: a file bind mount whose host
# side doesn't exist yet makes Docker create a directory in its place,
# and the first-run setup UI couldn't persist the config it writes.

FROM rust:1-bookworm AS build
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
VOLUME ["/data"]
EXPOSE 6789

ENTRYPOINT ["/usr/bin/tini", "--", "nzbd"]
CMD ["run", "--config", "/etc/nzbd/nzbd.toml", "--bind", "0.0.0.0:6789"]

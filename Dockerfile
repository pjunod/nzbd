# nzbd — multi-stage build: static-ish Rust binary + the PP toolchain.
#
#   docker build -t nzbd .
#   docker run -d -p 6789:6789 \
#     -v /data/usenet:/data -v ./nzbd.toml:/etc/nzbd/nzbd.toml nzbd

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
RUN useradd -r -u 1000 -m -d /var/lib/nzbd nzbd \
 && mkdir -p /data /etc/nzbd && chown nzbd /data /var/lib/nzbd
USER nzbd
VOLUME ["/data"]
EXPOSE 6789

ENTRYPOINT ["/usr/bin/tini", "--", "nzbd"]
CMD ["run", "--config", "/etc/nzbd/nzbd.toml", "--bind", "0.0.0.0:6789"]

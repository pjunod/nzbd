# Deploying nzbd

Every recipe below is complete — start from a blank machine, copy the
blocks in order, end with a running daemon. Adjust paths and passwords;
nothing else should need editing.

## Directories nzbd needs

nzbd touches exactly three kinds of places, all set in `nzbd.toml`:

| Directory | Config key | What lives there |
|---|---|---|
| Working root | `paths.main_dir` | in-progress downloads, the crash-safe queue journal (`<main_dir>/queue`), history DB |
| Completed | `paths.dest_dir` | finished, post-processed jobs (per-category subdirs/overrides) |
| Config | — | `nzbd.toml` itself (+ optional extension scripts dir, watch dir) |

In containers the convention is one volume, `/data`, holding both:
`main_dir = "/data"`, `dest_dir = "/data/complete"`, with the config
mounted read-only at `/etc/nzbd/nzbd.toml`.

**The path-alignment rule (the one people trip on):** Sonarr/Radarr must
see finished downloads at the *same path* nzbd reports. Mount the same
host directory at the same container path in both containers — e.g.
`-v /data/usenet:/data` on nzbd *and* on Sonarr. If the paths differ,
imports fail with "path does not exist".

## Docker, by hand

**Zero-config path:** skip writing a config entirely — mount an empty
config *directory* and let the first-run setup UI create the file:

```sh
sudo mkdir -p /data/usenet /opt/nzbd/config
sudo chown -R 1000:1000 /data/usenet /opt/nzbd/config
docker run -d --name nzbd --restart unless-stopped -p 6789:6789 \
  -v /data/usenet:/data \
  -v /opt/nzbd/config:/etc/nzbd \
  ghcr.io/pjunod/nzbd:latest
# → http://localhost:6789/ shows the setup form; it writes
#   /opt/nzbd/config/nzbd.toml and restarts the daemon with it.
```

Or fully declarative, config-first:

```sh
# 1. Host directories
sudo mkdir -p /data/usenet/complete /opt/nzbd

# 2. Config
sudo tee /opt/nzbd/nzbd.toml >/dev/null <<'EOF'
[paths]
main_dir = "/data"
dest_dir = "/data/complete"

[[server]]
name = "primary"
host = "news.example.com"
port = 563
tls = true
username = "CHANGE-ME"
password = "CHANGE-ME"
connections = 20

[[category]]
name = "tv"

[[category]]
name = "movies"

[api]
bind = "0.0.0.0:6789"
password = "CHANGE-ME"
EOF

# 3. Create + start the container
docker run -d \
  --name nzbd \
  --restart unless-stopped \
  -p 6789:6789 \
  -v /data/usenet:/data \
  -v /opt/nzbd/nzbd.toml:/etc/nzbd/nzbd.toml:ro \
  -e TZ=Etc/UTC \
  ghcr.io/pjunod/nzbd:latest

# 4. Verify
docker logs -f nzbd            # Ctrl-C to stop following
curl -s localhost:6789/healthz # -> ok
# Web UI: http://localhost:6789/  (user "nzbd", the [api] password)
```

The host volume is owned by the container's UID 1000; if your host dir
belongs to someone else: `sudo chown -R 1000:1000 /data/usenet`.

**Mind the order:** create the config file *before* the first
`docker run`. Docker turns a missing bind-mount source into an empty
*directory*, and the daemon will refuse to start with a "config path is
a DIRECTORY" error — remove the accidental directory on the host
(`rmdir /opt/nzbd/nzbd.toml`), write the file, and recreate the
container. (The Compose deployments are immune: they use compose
`configs`, which fail fast when the file is missing.)

Useful lifecycle commands:

```sh
docker exec -it nzbd nzbd status --url 127.0.0.1:6789   # queue as JSON
docker cp show.nzb nzbd:/tmp/ && docker exec nzbd nzbd add /tmp/show.nzb

# Upgrade to the latest image
docker pull ghcr.io/pjunod/nzbd:latest
docker stop nzbd && docker rm nzbd
# …then re-run the `docker run` block above (state is on the volumes)

# Build the image from a checkout instead of pulling
docker build -t nzbd . && docker run -d --name nzbd ... nzbd
```

Extension scripts: add `-v /opt/nzbd/scripts:/scripts:ro` and set
`post.scripts_dir = "/scripts"` in the config.

## Docker Compose

A ready deployment ships in
[`examples/docker-compose/`](../examples/docker-compose/) — the compose
file (with an optional Sonarr companion commented in) plus an example
config to copy:

```sh
git clone https://github.com/pjunod/nzbd.git
cd nzbd/examples/docker-compose
cp nzbd.toml.example nzbd.toml
$EDITOR nzbd.toml            # server credentials + [api] password

docker compose up -d
docker compose logs -f nzbd
curl -s localhost:6789/healthz   # -> ok
```

Edit the `volumes:` in the compose file if your downloads live somewhere
other than `/data/usenet`.

## Kubernetes

Complete manifests in [`examples/kubernetes/`](../examples/kubernetes/):
namespace, config Secret, PVC, Deployment (probes, non-root, Recreate
strategy), Service, kustomization.

```sh
cd examples/kubernetes
$EDITOR secret.yaml          # put your real nzbd.toml in stringData
$EDITOR pvc.yaml             # size + storageClassName
kubectl apply -k .

kubectl -n nzbd get pods
kubectl -n nzbd port-forward svc/nzbd 6789:6789
# http://localhost:6789/ — in-cluster clients use nzbd.nzbd.svc:6789
```

Keep `replicas: 1` — the queue journal lives on the RWO volume. Scaling
out means nzbd clustering (next section), not more replicas; the
Kubernetes shape for that is described in the examples'
[README](../examples/kubernetes/README.md).

## systemd (bare metal)

Unit file in [`examples/systemd/nzbd.service`](../examples/systemd/nzbd.service):

```sh
sudo useradd -r -m -d /var/lib/nzbd nzbd
sudo install -m 755 nzbd /usr/local/bin/          # binary from INSTALL.md
sudo mkdir -p /etc/nzbd /data && sudo chown nzbd /data
sudo cp nzbd.toml /etc/nzbd/
sudo cp examples/systemd/nzbd.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now nzbd
systemctl status nzbd && journalctl -u nzbd -f
```

The unit is hardened (`ProtectSystem=strict`); if your download dirs are
not under `/data`, extend `ReadWritePaths=` accordingly.

## Multi-node cluster (shared volume)

Design + failure matrix: [CLUSTERING.md](CLUSTERING.md). Requirements: a
shared POSIX filesystem mounted at the same path on every node (Gluster
with quorum is the reference; NFSv4/CephFS also qualify — see
CLUSTERING.md §12), and one shared secret.

```sh
# On ONE machine: mint the cluster secret, then copy it to every node
openssl rand -hex 32 | sudo tee /etc/nzbd/cluster.secret >/dev/null
sudo chmod 600 /etc/nzbd/cluster.secret
```

Each node runs the normal single-node setup (any recipe above) plus a
`[cluster]` block — identical everywhere except `node_name` and
`advertise_url`:

```toml
# node-a (10.0.0.11)
[cluster]
enabled = true
node_name = "node-a"
shared_dir = "/mnt/work"                  # the shared mount, same path everywhere
advertise_url = "http://10.0.0.11:6789"
secret_file = "/etc/nzbd/cluster.secret"
```

```toml
# node-b (10.0.0.12) — e.g. a box that should post-process but not download
[cluster]
enabled = true
node_name = "node-b"
shared_dir = "/mnt/work"
advertise_url = "http://10.0.0.12:6789"
secret_file = "/etc/nzbd/cluster.secret"
download = false          # role knobs: download / post_process /
post_process = true       # coordinator / priority — CONFIGURATION.md
```

Start the nodes in any order. Verify:

```sh
curl -s http://10.0.0.11:6789/api/v1/cluster | jq
# nodes, roles, and the current leader; run it against any node
```

Operationally: point the *arr apps at any node (each proxies to the
leader), restart nodes freely (leases expire and are adopted — nothing
already downloaded is re-fetched), and keep Gluster quorum on so the
volume itself never splits.

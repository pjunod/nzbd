# nzbd on Kubernetes

A minimal, production-shaped single-node deployment: one replica, config
from a Secret (it contains news-server credentials), downloads on a PVC,
health probes on `/healthz`.

```sh
# 1. Edit secret.yaml — put your real nzbd.toml in it
# 2. Size the PVC in pvc.yaml for your storage class
kubectl apply -k .
kubectl -n nzbd port-forward svc/nzbd 6789:6789   # then open http://localhost:6789/
```

Files:

| File | Purpose |
|---|---|
| `namespace.yaml` | the `nzbd` namespace |
| `secret.yaml` | the full `nzbd.toml` (Secret, not ConfigMap — it holds server passwords) |
| `pvc.yaml` | `/data` volume for downloads + state |
| `deployment.yaml` | the daemon: probes, resources, non-root security context |
| `service.yaml` | ClusterIP on 6789 |
| `kustomization.yaml` | ties it together (`kubectl apply -k .`) |

Notes:

- **One replica.** nzbd keeps its queue journal under `paths.main_dir`;
  two pods over the same RWO volume would fight. Scale *out* with nzbd's
  own clustering instead (below), not with `replicas: 2`.
- Point Sonarr/Radarr (in-cluster) at `nzbd.nzbd.svc:6789` as an NZBGet
  client. For outside access add an Ingress/LoadBalancer in front of the
  Service — and set an `[api] password` first.
- The *arr apps must see the same paths as nzbd to import finished
  downloads: mount this PVC (or the same underlying storage) into them at
  the same mount point (`/data`).

## Multi-node nzbd cluster on Kubernetes

nzbd clustering needs a shared **RWX POSIX volume** (CephFS, NFS,
GlusterFS — see `docs/CLUSTERING.md` §12 for what the volume must
guarantee). The shape: one Deployment **per nzbd node**, each with

- the shared RWX PVC mounted at the same path (e.g. `/mnt/work`),
- its own `nzbd.toml` differing only in `node_name` (unique + stable) and
  `advertise_url` (its own Service DNS name),
- the same `[cluster] secret` (one shared Secret),
- its own ClusterIP Service, so peers can reach it by name.

Any node's Service can be the target for the *arr apps — every node
proxies to the current leader. `deployment.yaml` has commented markers at
the two places that change per node.

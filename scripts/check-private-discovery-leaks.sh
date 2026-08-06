#!/usr/bin/env bash
set -euo pipefail

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "private discovery packet capture requires Linux" >&2
  exit 1
fi

for command in cargo getent grep iptables ip6tables od sed sudo tcpdump tr; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "missing required command: $command" >&2
    exit 1
  fi
done

probe_port=45123
runner_uid="$(id -u)"
runner_user="$(id -un)"
capture_file="$(mktemp)"
capture_log="$(mktemp)"
test_log="$(mktemp)"
capture_hex_file="$(mktemp)"
tcpdump_pid=""
capture_stopped=0
rules_installed=0

cleanup() {
  set +e
  if [[ -n "$tcpdump_pid" && "$capture_stopped" -eq 0 ]]; then
    sudo kill -INT "$tcpdump_pid" >/dev/null 2>&1
    wait "$tcpdump_pid" >/dev/null 2>&1
  fi
  if [[ "$rules_installed" -eq 1 ]]; then
    for port in 6881 25401; do
      sudo iptables -t nat -D OUTPUT -p udp -m owner --uid-owner "$runner_uid" --dport "$port" -j REDIRECT --to-ports "$probe_port" >/dev/null 2>&1
      sudo ip6tables -D OUTPUT -p udp -m owner --uid-owner "$runner_uid" --dport "$port" -j REJECT >/dev/null 2>&1
    done
  fi
  rm -f "$capture_file" "$capture_log" "$test_log" "$capture_hex_file"
}
trap cleanup EXIT

# Force both stable-8.1.1 bootstrap names through DNS before installing the
# redirect. The DHT never reaches those hosts: IPv4 is redirected to the local
# probe, while matching IPv6 bootstrap traffic is blocked before the device
# capture point and is therefore deliberately unobserved.
getent ahostsv4 dht.transmissionbt.com >/dev/null
getent ahostsv4 dht.libtorrent.org >/dev/null

# Keep tcpdump's capture process under the runner account so it can open the
# runner-owned mktemp file after dropping root privileges.
sudo tcpdump -Z "$runner_user" -i any -U -s 0 -w "$capture_file" udp >"$capture_log" 2>&1 &
tcpdump_pid=$!
for _ in {1..50}; do
  if grep -q "listening on" "$capture_log"; then
    break
  fi
  if ! kill -0 "$tcpdump_pid" >/dev/null 2>&1; then
    cat "$capture_log" >&2
    exit 1
  fi
  sleep 0.1
done
if ! grep -q "listening on" "$capture_log"; then
  echo "tcpdump did not become ready" >&2
  cat "$capture_log" >&2
  exit 1
fi

rules_installed=1
for port in 6881 25401; do
  sudo iptables -t nat -I OUTPUT 1 -p udp -m owner --uid-owner "$runner_uid" --dport "$port" -j REDIRECT --to-ports "$probe_port"
  sudo ip6tables -I OUTPUT 1 -p udp -m owner --uid-owner "$runner_uid" --dport "$port" -j REJECT
done

NZBD_PRIVATE_DISCOVERY_CAPTURE=1 \
NZBD_DHT_PROBE_PORT="$probe_port" \
  cargo test --locked -p nzbd-torrent --test private_discovery_capture \
    private_torrent_never_queries_dht_when_the_session_dht_is_live \
    -- --ignored --exact --nocapture | tee "$test_log"

sudo kill -INT "$tcpdump_pid"
wait "$tcpdump_pid" || true
capture_stopped=1

public_hash="$(sed -n 's/.*NZBD_PUBLIC_INFO_HASH=\([0-9a-f]\{40\}\).*/\1/p' "$test_log" | tail -n 1)"
window_control_hash="$(sed -n 's/.*NZBD_WINDOW_CONTROL_INFO_HASH=\([0-9a-f]\{40\}\).*/\1/p' "$test_log" | tail -n 1)"
private_hash="$(sed -n 's/.*NZBD_PRIVATE_INFO_HASH=\([0-9a-f]\{40\}\).*/\1/p' "$test_log" | tail -n 1)"
if [[ -z "$public_hash" || -z "$window_control_hash" || -z "$private_hash" ]]; then
  echo "test did not report all three packet-capture markers" >&2
  exit 1
fi

od -An -v -tx1 "$capture_file" | tr -d ' \n' >"$capture_hex_file"

if ! grep -Fq "$public_hash" "$capture_hex_file"; then
  echo "public DHT control hash was absent from the packet capture" >&2
  exit 1
fi
if ! grep -Fq "$window_control_hash" "$capture_hex_file"; then
  echo "private-window DHT control hash was absent from the packet capture" >&2
  exit 1
fi
if grep -Fq "$private_hash" "$capture_hex_file"; then
  echo "private info hash appeared as binary data in captured UDP traffic" >&2
  exit 1
fi

private_ascii_hex="$(printf '%s' "$private_hash" | od -An -v -tx1 | tr -d ' \n')"
private_upper_ascii_hex="$(printf '%s' "${private_hash^^}" | od -An -v -tx1 | tr -d ' \n')"
if grep -Fq "$private_ascii_hex" "$capture_hex_file" || grep -Fq "$private_upper_ascii_hex" "$capture_hex_file"; then
  echo "private info hash appeared as text in captured UDP traffic" >&2
  exit 1
fi

echo "private discovery capture passed: DHT controls observed before and during the private window; no private DHT/LSD hash observed"

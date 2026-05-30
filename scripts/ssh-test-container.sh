#!/usr/bin/env bash
# Ephemeral sshd container used as the target for the gated SSH tests
# (the russh API spike and the real-SSH integration test). Local/test-only:
# throwaway credentials, an ephemeral keypair, published on 127.0.0.1 only.
#
#   scripts/ssh-test-container.sh up      # build image, (re)start container, print env
#   scripts/ssh-test-container.sh down    # stop + remove the container
#   scripts/ssh-test-container.sh env     # print the connection env vars
#
# Then, for the spike / gated test:
#   eval "$(scripts/ssh-test-container.sh env)"
#
# The keypair lives under $HOME (native fs) rather than the repo, because on a
# /mnt/c Windows mount chmod does not stick and the openssh client rejects the
# private key as "too open".
set -euo pipefail

IMAGE=ordius-ssh-test
NAME=ordius-ssh-test
PORT="${ORDIUS_SSH_TEST_PORT:-2222}"
USER_NAME=ordius
PASSWORD=ordius
KEYDIR="${ORDIUS_SSH_TEST_HOME:-$HOME/.ordius-ssh-test}"
KEY="$KEYDIR/id_ed25519"

ensure_key() {
    mkdir -p "$KEYDIR"
    if [ ! -f "$KEY" ]; then
        ssh-keygen -t ed25519 -N '' -C ordius-ssh-test -f "$KEY" >/dev/null
    fi
    chmod 700 "$KEYDIR"
    chmod 600 "$KEY"
}

build() {
    ensure_key
    cat >"$KEYDIR/Dockerfile" <<'EOF'
FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends openssh-server ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && useradd -m -s /bin/bash ordius \
 && echo 'ordius:ordius' | chpasswd \
 && mkdir -p /run/sshd /home/ordius/.ssh \
 && sed -i -e 's/#\?PasswordAuthentication.*/PasswordAuthentication yes/' \
           -e 's/#\?PubkeyAuthentication.*/PubkeyAuthentication yes/' \
           -e 's/#\?AllowTcpForwarding.*/AllowTcpForwarding yes/' \
           /etc/ssh/sshd_config
ARG PUBKEY
RUN printf '%s\n' "$PUBKEY" > /home/ordius/.ssh/authorized_keys \
 && chown -R ordius:ordius /home/ordius/.ssh \
 && chmod 700 /home/ordius/.ssh \
 && chmod 600 /home/ordius/.ssh/authorized_keys
EXPOSE 22
CMD ["/usr/sbin/sshd","-D","-e"]
EOF
    docker build --build-arg PUBKEY="$(cat "$KEY.pub")" -t "$IMAGE" "$KEYDIR" >&2
}

up() {
    build
    docker rm -f "$NAME" >/dev/null 2>&1 || true
    docker run -d --name "$NAME" -p "127.0.0.1:$PORT:22" "$IMAGE" >&2
    for _ in $(seq 1 30); do
        if ssh -i "$KEY" -p "$PORT" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
            -o ConnectTimeout=2 -o BatchMode=yes "$USER_NAME@127.0.0.1" true 2>/dev/null; then
            echo "sshd ready on 127.0.0.1:$PORT" >&2
            env_vars
            return 0
        fi
        sleep 1
    done
    echo "error: sshd did not become reachable on 127.0.0.1:$PORT" >&2
    docker logs "$NAME" >&2 || true
    exit 1
}

down() {
    docker rm -f "$NAME" >/dev/null 2>&1 || true
}

env_vars() {
    echo "export ORDIUS_TEST_SSH_HOST=$USER_NAME@127.0.0.1:$PORT"
    echo "export ORDIUS_TEST_SSH_KEY=$KEY"
    echo "export ORDIUS_TEST_SSH_PASSWORD=$PASSWORD"
}

case "${1:-up}" in
up) up ;;
down) down ;;
env) env_vars ;;
build) build ;;
*)
    echo "usage: $0 {up|down|env|build}" >&2
    exit 2
    ;;
esac

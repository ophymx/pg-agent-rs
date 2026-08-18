# Acceptance-test node image: a systemd-booted Debian-family base with
# PostgreSQL + the real pg-agent-rs .deb, run with systemd as PID 1 so
# the packaged unit, polkit rule, sd_notify contract, and validate-env
# gate are all exercised exactly as in production.
#
# Parameterised for the OS/PostgreSQL matrix (testing/matrix.sh). Each
# base is paired with the PostgreSQL version it ships NATIVELY — no
# PGDG repo — so the matrix tests the combinations distributions
# actually deliver, and a build failure means "this pairing does not
# exist" rather than "the extra repo moved".
#
# Build context must contain pg-agent.deb (copied in by
# testing/acceptance.sh from dist/).
ARG BASE_IMAGE=debian:trixie
FROM ${BASE_IMAGE}

# Re-declared: an ARG before FROM is out of scope in the build stage.
ARG PG_VERSION=17
ENV PG_VERSION=${PG_VERSION}
ENV DEBIAN_FRONTEND=noninteractive

# ALSO on disk, because the image's ENV does not reach the provisioning
# unit: systemd (PID 1) gives its services a clean environment, so
# `PG_VERSION` is set for `docker exec` and unset for anything systemd
# starts. Under provision.sh's `set -u` that aborted the script inside
# a heredoc and left a config.toml with an EMPTY pool — which surfaced
# three layers away as "pgpool_node_id specifies id=0 which is not in
# pool". A file is read the same way by both.
RUN mkdir -p /etc/pg-agent-matrix \
    && echo "PG_VERSION=${PG_VERSION}" > /etc/pg-agent-matrix/env

# Mask pgpool2 BEFORE any install, per BOOTSTRAP.md Phase 1.1 — the
# Debian postinst would otherwise enable + start it. (Masking works in
# a build layer: it is just a symlink to /dev/null.)
RUN ln -s /dev/null /etc/systemd/system/pgpool2.service

RUN apt-get update && apt-get install -y --no-install-recommends \
        systemd systemd-sysv dbus polkitd \
        postgresql-${PG_VERSION} postgresql-${PG_VERSION}-pgpool2 pgpool2 \
        openssl procps less curl iptables \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

COPY pg-agent.deb /tmp/pg-agent.deb
RUN apt-get update && apt-get install -y /tmp/pg-agent.deb \
    && rm /tmp/pg-agent.deb && apt-get clean && rm -rf /var/lib/apt/lists/*

# Polkit rule granting the postgres user unit management over the
# PostgreSQL and pgpool units — in production Ansible installs this
# (the agent deliberately owns no /etc files; see systemd.rs docs).
COPY 50-pg-agent.rules /etc/polkit-1/rules.d/50-pg-agent.rules

# Per-node provisioning at boot (config.toml, TLS, pg_hba, roles…),
# plus the operator-run pgpool setup (phase 2 scenarios).
COPY provision.sh /usr/local/sbin/pg-agent-provision
COPY pgpool-setup.sh /usr/local/sbin/pg-agent-pgpool-setup
COPY provision.service /etc/systemd/system/pg-agent-provision.service
RUN chmod 0755 /usr/local/sbin/pg-agent-provision /usr/local/sbin/pg-agent-pgpool-setup \
    && systemctl enable pg-agent-provision.service \
    # PostgreSQL does not auto-start: provision starts it on the
    # bootstrap primary only; standbys get theirs via ClusterInit
    # (three fresh initdb'd primaries would otherwise all boot as TL1
    # and trip the phantom-primary check — see testing/README.md).
    && systemctl disable postgresql.service \
    # pg_agentd: enabled; ordering (After=postgresql) is satisfied
    # trivially when postgres is left stopped.
    && systemctl enable pg_agentd.service

STOPSIGNAL SIGRTMIN+3
CMD ["/sbin/init"]

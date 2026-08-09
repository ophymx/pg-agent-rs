# Acceptance-test node image: Debian 13 + systemd + PostgreSQL 17 +
# the real pg-agent-rs .deb, booted with systemd as PID 1 so the
# packaged unit, polkit rule, sd_notify contract, and validate-env
# gate are all exercised exactly as in production.
#
# Build context must contain pg-agent.deb (copied in by
# testing/acceptance.sh from dist/).
FROM debian:trixie

ENV DEBIAN_FRONTEND=noninteractive

# Mask pgpool2 BEFORE any install, per BOOTSTRAP.md Phase 1.1 — the
# Debian postinst would otherwise enable + start it. (Masking works in
# a build layer: it is just a symlink to /dev/null.)
RUN ln -s /dev/null /etc/systemd/system/pgpool2.service

RUN apt-get update && apt-get install -y --no-install-recommends \
        systemd systemd-sysv dbus polkitd \
        postgresql-17 postgresql-17-pgpool2 pgpool2 \
        openssl procps less curl \
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

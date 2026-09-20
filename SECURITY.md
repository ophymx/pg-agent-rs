# Security policy

## Reporting a vulnerability

Please report suspected vulnerabilities through GitHub's private
vulnerability reporting: the **Security** tab of this repository →
**Report a vulnerability**. That channel is private to the maintainers
until a fix is published.

Please do **not** open a public issue for a suspected vulnerability.

A useful report says what an attacker gains, not only what the code
does. The most valuable ones here describe a sequence of node states —
what was up, what was partitioned, who held the lease — because that is
the shape almost every real problem in this project has taken.

Expect a first response within a week. This is maintained by one
person; if a report goes unanswered, it was missed rather than ignored,
and a polite nudge on the same thread is welcome.

## What is in scope

Anything that lets an attacker reach or subvert the cluster:

- **The peer mesh.** Every daemon-to-daemon RPC is mTLS against a SAN
  allowlist built from the configured pool. Anything that accepts a
  peer it should not, or that would let a non-member issue peer RPCs,
  is in scope.
- **The local socket.** `pg_agentc` and `pg_agentctl` reach the daemon
  over a Unix socket, which is the trust boundary for every operator
  and hook command — the daemon holds the PCP credentials and the cert
  material precisely so those callers do not need them. Anything that
  widens who can drive that socket is in scope.
- **The hook surface.** `pg_agentc` is invoked by pgpool's
  `pgpool_recovery` C extension and by PostgreSQL's `restore_command`,
  with arguments this project does not control. Argument handling that
  can be made to execute something, or to write outside `$PGDATA`, is
  in scope.
- **Privilege boundaries.** The daemon runs as `postgres` and reaches
  systemd over D-Bus through a polkit rule scoped to specific units.
  A path to acting outside that scope is in scope.
- **Correctness failures with a safety consequence**, notably anything
  that produces two primaries accepting writes, or that acknowledges a
  write which a subsequent promotion discards. These are treated as
  security issues here regardless of whether an attacker can trigger
  them, because the consequence is the same.

## What is not in scope

- **pgpool-II, PostgreSQL, or HAProxy themselves.** Report those
  upstream.
- **Deployments that skip `pg_agentd validate-env`.** It is the
  `nginx -t` equivalent and is wired as `ExecStartPre=` in the shipped
  unit; it refuses a pool smaller than three nodes, missing or
  world-readable TLS material, and a PostgreSQL unit systemd may
  restart behind the agent's back. Findings that require disabling it
  are configuration, not vulnerabilities.
- **The absence of NSS support.** A static musl binary resolves through
  DNS and `/etc/hosts` and not `nsswitch.conf`. That is a stated
  boundary, not a defect.
- **Anything requiring an attacker who already has the `postgres`
  account** on a cluster node. At that point they own the database.

## Supported versions

The latest release only. This project has not been through wide
production use and there is no back-porting capacity; fixes land on
`main` and go out in the next release.

See the Status section of [README.md](README.md) for an honest account
of maturity before you rely on this.

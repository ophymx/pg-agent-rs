# Contributing

Contributions are welcome. This file is short because most of what you
need is `./scripts/precommit-check.sh` and a willingness to be specific.

## Before you write much

**Open an issue first for anything that changes behaviour.** Not
ceremony — this project's hard parts are its refusals, and a change
that looks like a small improvement is often removing a refusal that
exists for a reason nobody wrote down clearly enough. A paragraph
beforehand saves a rewritten branch.

Small things — a failing edge case, a doc line that is wrong, a
confusing error message — just send them.

## The checks

```
./scripts/precommit-check.sh          # fmt, clippy -D warnings, tests, docs
./scripts/precommit-check.sh --fix    # apply what can be applied
./scripts/precommit-check.sh --quick  # skip cargo doc, the slow one
```

CI runs the same script, so a green run locally is a green run there.
The toolchain is pinned in `rust-toolchain.toml` and the pin is
load-bearing: `clippy -D warnings` makes every new lint in every new
Rust release a build break, and a floating toolchain means that break
lands on whoever is unluckiest. Do not bump it as a drive-by.

A fresh checkout needs nothing but a Rust toolchain. If you find
yourself installing a system package to build, that is a bug — tell us.

## Tests

Unit tests for logic; the acceptance suite for anything about how nodes
behave toward each other.

```
testing/acceptance.sh              # baseline cell, ~12 minutes
FAIL_FAST=1 testing/acceptance.sh  # stop at the first failing check
testing/matrix.sh                  # every OS / PostgreSQL cell, ~50 minutes
```

It needs Docker with privileged containers running systemd as PID 1.
Run at least the baseline cell for any change to the agent, the
executors, or consensus. `testing/README.md` explains the scenarios.

**Assert on event order, not on sampled instants.** The harness tails
every node's journal into one ordered log for exactly this reason. A
test that polls until it sees the state it wanted will pass on a
cluster that reached it by an illegal route, and the suite has caught
real bugs precisely by refusing to accept that.

If a scenario fails, read what it asserted before changing it. Three
gates in this suite went stale because the code got better and the
assertions described the older, worse behaviour — which is a fine
reason to change a test, and is not the same as a test being wrong.

## What this project cares about

Worth knowing, because reviews will come back to these:

- **`Err` means unknown, never false.** Treating "I could not find
  out" as "it is not so" is how split-brain happens. If you add a
  failure path, decide explicitly what its caller is entitled to
  conclude.
- **Refusals carry their reason and the way out.** An error message
  that says what is wrong without saying what to do next is half
  finished. Several commands exist mainly for their refusals.
- **Comments say why, not what.** The code says what it does. Comments
  are for the constraint that is not visible from here — the failure
  that motivated a check, the reason an obvious simplification is
  wrong.
- **Documentation that is wrong is worse than absent.** If your change
  makes a line in `SPEC.md`, `README.md`, `docs/` or `testing/README.md`
  untrue, fixing it is part of the change.

## Commits

Describe the behaviour change and why it matters. The existing log is
the style guide; the subject lines are sentences about what was wrong,
not labels.

Keep unrelated changes in separate commits.

## Licence

By contributing you agree your work is licensed under the same terms as
the project: MIT or Apache-2.0, at the user's option.

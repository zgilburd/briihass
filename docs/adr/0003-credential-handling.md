# ADR-0003: Credential handling — secrets outside the repo, injected at runtime

- **Status:** Accepted
- **Date:** 2026-05-18

## Context

briihass needs a handful of credential sets at runtime:

1. **vRIoT** controller mgmt API user (if you talk to the controller).
2. **vSmartZone** controller API user (reserved; not consumed in v1 per
   [ADR-0005](0005-vsz-enrichment.md)).
3. **`INGEST_SHARED_SECRET`** — the value the BLE Scan plugin presents in its
   `Api-Key` header (per [ADR-0001](0001-http-post-ingest.md)).
4. **Mosquitto** user/password the bridge uses to publish.
5. **`BRIIHASS_POSTGRES_DSN`** — the Postgres connection string.

Topology (which beacons are tracked, which APs map to which zones) is **not** a
file or a credential — it lives in Postgres and is mutated through the admin UI
(see [ADR-0007](0007-postgres-resident-persistence.md)).

Two consumption contexts:
- **Production runtime** — the process needs these as environment variables at
  startup.
- **Local dev** — running the bridge locally against real or test backends.

## Decision

**Everything is supplied as environment variables; no secret is ever committed
to the repo.**

**Production runtime:** inject the env vars from whatever secret manager your
platform already uses — e.g. sealed/encrypted secrets stored in your
infrastructure repo, a cloud secret manager, or your orchestrator's native
secret object. briihass does not care which; it only reads `os.Getenv`.

**Local dev:** a single plaintext file outside the repo at
`~/.config/briihass/credentials.env` (mode 600), managed by
`scripts/dev-creds.sh` (`--check`, `--edit`, `--print`, `--path`). Source it
into your shell (`eval "$(scripts/dev-creds.sh --print)"`) before `make run`.

**Outside the repo, full stop.** No encrypted-in-repo secret blob.

## Rationale

- **Plaintext outside the repo can never be accidentally committed.**
  `git add -A`, a misconfigured editor, a stray `git stash` — none of them can
  pull a file from `~/.config/briihass/` into the working tree.
- **No bespoke crypto tooling for dev.** Disk encryption is the right layer for
  at-rest protection on a dev machine; the same protection already covers your
  SSH keys, kubeconfig, and cloud credentials. Per-app encryption on top buys
  little and costs friction.
- **No secret ever transits stdout.** `dev-creds.sh --print` emits only a
  `set -a; source <path>; set +a` line to `eval`, keeping values in env vars,
  not on the terminal.
- **Platform-agnostic prod.** Reading env vars means any secret manager works;
  the bridge has no hard dependency on a specific one.

## Consequences

**Positive**
- Zero new tooling required for dev.
- Trivial to rotate: edit the file (dev) or rotate in your secret manager and
  restart the process (prod).
- The published repo contains no secrets and no environment-specific secret
  wiring.

**Negative**
- Dev creds are visible in plaintext to anyone with shell as your user on the
  dev machine. Mitigated by mode 600 on the file, mode 700 on the directory,
  full-disk encryption, and not running untrusted code as your user (the same
  rule that protects your SSH keys).
- Moving to a new dev machine is a manual secure copy of the file; there is no
  encrypted-in-repo path to clone from.

## What `dev-creds.sh` does

| Subcommand | Behavior |
|---|---|
| `--check` (default) | Verifies file exists, perms are 600, required keys present. |
| `--edit` | Opens file in `$EDITOR`; seeds a template on first run; re-applies 600 on exit. |
| `--print` | Echoes a `set -a; source <path>; set +a` line for shell eval. |
| `--path` | Echoes the absolute path. |

The script intentionally does not implement encryption. If you want encryption
at rest, encrypt the disk.

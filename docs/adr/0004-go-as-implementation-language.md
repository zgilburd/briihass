# ADR-0004: Implement briihass in Go

- **Status:** Accepted
- **Date:** 2026-05-18

## Context

The bridge is a long-lived daemon doing modest work: receive HTTP POSTs,
parse short hex strings, call a REST API (cached), publish MQTT messages,
maintain a small in-memory state machine. Resource footprint should be
minimal; the container should be small and have a tiny attack surface;
deploys should be fast.

Candidate languages considered: Go, TypeScript/Node, Python, Rust.

## Decision

**Go.** Single static binary, CGO disabled, multi-stage Dockerfile with a
distroless final image.

## Rationale

- **Static binary on distroless** = the smallest production-grade attack
  surface available without going to scratch. No interpreter, no package
  manager, no shell in the running container.
- **Concurrency primitives match the workload.** One goroutine per POST
  handler, a goroutine for the vSZ cache refresher, a goroutine for the
  MQTT publisher with a bounded channel between presence and publish.
  Idiomatic, easy to test, easy to reason about under load.
- **Standard library covers the dependencies.** `net/http` for ingest,
  `crypto/hmac` for auth, `encoding/hex` and `encoding/binary` for advert
  parsing. The only third-party dependency expected is an MQTT client
  (`eclipse/paho.mqtt.golang`).
- **Fast feedback loop.** Sub-second `go build` and `go test` keeps
  iteration tight. Compile-time type checking catches a class of bugs that
  Python and lightly-typed JS let through.
- **Operator familiarity.** Lines up with the surrounding cloud-native
  ecosystem (Kubernetes, ingress, GitOps, Mosquitto exporters).

## Considered and rejected

- **TypeScript/Node 22.** Matches the user's typical stack but produces
  larger images, has a heavier runtime, and the MQTT + HTTP-server combo
  doesn't get materially easier than in Go for this scope.
- **Python 3.12.** Best BLE-advert parsing ecosystem, but we only parse
  one well-known advert shape — no library win. Larger image, slower cold
  start, weaker static guarantees.
- **Rust.** Smallest binary, strongest correctness story. Rejected on
  iteration speed for a small bridge: the marginal correctness gain over
  Go does not outweigh the slower build and steeper LSP feedback for the
  size of this codebase.

## Consequences

**Positive**
- Container probably <20 MB. Memory footprint <50 MB at steady state.
- Cross-compilation is trivial if we ever need to target a different arch.
- Easy to introduce a small `pprof` endpoint behind the admin port for
  on-demand profiling.

**Negative**
- The Go MQTT client ecosystem is smaller than Python's. `paho.mqtt.golang`
  is the de facto choice; we accept the dependency.
- Generics are recent; some idioms (e.g., the LRU cache for vSZ
  enrichment) require either generics or a small typed wrapper. Acceptable.

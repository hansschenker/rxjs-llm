# ADR-0023: No persistence layer — snapshot/restore only

**Status:** accepted · **Decision:** D5.4

## Context

Every host persists differently: localStorage, Postgres, a KV store, a
file. A persistence abstraction would mean drivers, serialization
options, and migration machinery — a framework, in a module whose target
is ~150 lines.

## Decision

`memory.snapshot(): MemorySnapshot` returns plain serializable data
(`{ turns }` — a defensive copy), and `createMemory({ restore })` starts
from one. That is the entire persistence surface; hosts persist however
they like. Pinned by property test: for any turn sequence and any pure
view, `restore(snapshot(m))` yields an equivalent view and an equal
snapshot.

**Turns are the truth; derived state is not snapshotted.** A summary
view's running summary is a cache of the turns — a restored memory with a
summary view re-folds lazily when the threshold trips, by design. The
alternative (serializing view-internal state) would couple the snapshot
format to every strategy's internals and break strategy-swapping on
restore, which ADR-0020 exists to allow. A host that wants to avoid
re-fold cost can persist the rendered `view()` messages itself.

## Consequences

- Snapshots survive strategy changes: restore into a different view.
- `NON_GOALS.md` already lists the persistence layer as out of scope;
  this ADR is the reasoned version of that line.

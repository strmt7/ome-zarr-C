# Architecture-First Porting

Architecture is priority number 1 in this repository. A native port only counts
if it preserves the repo's structure and leaves a clear proof path behind it.

## Required layers

1. Frozen reference:
   `source_code_v.0.15.0/` is the immutable upstream truth source.
2. Pure-native semantic core:
   `cpp/native/` should hold the actual converted behavior whenever the logic
   is deterministic and self-contained.
3. Minimal binding layer:
   `cpp/bindings/` should expose Python-facing entry points and nothing more.
4. Thin compatibility wrapper:
   `ome_zarr_c/` should mostly adapt imports and unavoidable runtime
   integration points.
5. Differential proof:
   `tests/` must compare the frozen upstream behavior against the converted
   behavior on the same runtime.

Existing mixed files under `cpp/` that combine semantics and Python glue are
not grandfathered in. They are migration debt and should be split.

## Conversion order

Port in this order unless there is a compelling reason not to:

1. Pure computational helpers.
2. Deterministic Python-object behavior that can still be proven locally.
3. External-boundary behavior that can be patched at the boundary and compared
   by outbound calls or printed output.
4. Persistent-store and filesystem mutation only when the live runtime is
   healthy enough to prove serialized effects end to end.

## What worked in this port

- Small self-contained slices were the safest path: conversions, axes, format
  helpers, Dask utilities, scale helpers, and synthetic-data helpers all became
  tractable once isolated.
- Differential tests with randomized cases were useful for unbounded inputs
  such as CSV value parsing and coordinate transformations.
- Path-stable fixtures mattered for read-only utilities that print or serialize
  paths.
- Reinstalling the editable package after each native edit prevented false
  confidence from stale extension binaries.
- Thin wrappers made parity bugs easier to spot. A recent example was numeric
  metadata version coercion in `format`, which was fixed by moving the decision
  logic into the native layer and testing the edge case explicitly.

## What failed or blocked progress

- This runtime currently hangs on local and in-memory Zarr store operations.
  Any surface that requires live `zarr.open_group`, `zarr.open_array`, or
  equivalent store-backed mutation remains architecturally blocked here.
- Build isolation can try to reach the network for build dependencies. In a
  constrained environment, rebuild with the already-installed toolchain instead
  of treating the rebuild as optional.
- Wide "run everything" claims are unsafe when known blocked lanes exist.
  Prefer an explicit proven-safe suite and document what is excluded.
- Documentation can drift faster than code. If README or reference docs over-
  claim verified coverage, fix the docs immediately.

## Architectural do's

- Keep behavior in pure C++ and compatibility at the boundary only.
- Separate pure logic from store mutation when designing the port.
- Treat runtime blockers as a stop sign for parity claims.
- Update the docs whenever the proof boundary changes.
- Move mixed legacy code toward `cpp/native/` plus `cpp/bindings/`, not toward
  a larger monolithic mixed-export file like the historical `core.cpp`
  pattern.

## Architectural don'ts

- Do not use a wrapper-heavy design to hide unported logic.
- Do not add new Python-integrated semantics to mixed C++ files just because
  they are already impure.
- Do not count blocked store-backed surfaces as done because a partial wrapper
  exists.
- Do not let mocked tests stand in for live persistent-store parity.
- Do not broaden the port into larger surfaces before the smaller slice is both
  proven and documented.

# Porting Contract

This repository exists to port `ome/ome-zarr-py` release `v0.15.0` to C++
incrementally while keeping the imported upstream snapshot intact.

## Hard boundaries

- `source_code_v.0.15.0/` is immutable reference code.
- All new implementation work happens in `cpp/`, `ome_zarr_c/`, `tests/`,
  `docs/`, and related repo-local support files.
- Do not silently "improve" upstream behavior during a parity port.
- If upstream has a quirk or bug and parity is the goal, preserve it until a
  separately documented change deliberately fixes it.

## Required porting sequence

1. Pick the smallest self-contained upstream surface.
2. Read the exact upstream implementation and nearest tests.
3. Write differential tests against the frozen snapshot first or alongside the
   port.
4. Implement the C++ core.
5. Add only the thinnest Python wrapper needed for compatibility.
6. Run the narrowest relevant parity suites.
7. Only after parity is proven, run benchmarks on representative data.

## Evidence rules

- Bounded state space:
  Exhaustive differential tests are required before claiming full parity.
- Unbounded state space:
  Use boundary cases, randomized differential tests, and real-data checks. Do
  not claim mathematical 100% coverage when the input space is effectively
  unbounded.
- Performance:
  Use the same inputs, same machine class, same environment, and repeated runs.
  Report medians and version details.

## Allowed local deviations

- Build-system files
- CI workflows
- Agent docs and skills
- Benchmarks and fixtures
- Compatibility wrappers that preserve upstream public behavior

## Not-done conditions

- The C++ path works but the upstream parity test is missing.
- The test only checks the happy path when the upstream surface has meaningful
  edge cases.
- A performance claim exists without measured data.
- A change required editing the frozen snapshot.

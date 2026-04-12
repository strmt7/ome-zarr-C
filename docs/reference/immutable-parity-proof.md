# Immutable Parity Proof

Parity claims in this repository should leave behind evidence that can be
rechecked later without trusting memory or hand-written summaries.

## Frozen upstream immutability

- `source_code_v.0.15.0/` stays immutable as the reference oracle.
- `scripts/frozen_source_manifest.py` generates and verifies a committed
  SHA256 manifest for the frozen snapshot.
- The snapshot protection workflow should reject both direct Git edits to the
  frozen snapshot and manifest mismatches against the committed content hash.

## Immutable parity evidence

Use the smallest proof that matches the surface:

- Deterministic object behavior:
  compare stable JSON-like snapshots of upstream and native outcomes.
- Persistent store or filesystem mutation:
  compare serialized output trees or per-file hashes, not only return values.
- External side effects:
  patch the boundary and compare the outbound call payloads.
- Read-only path-sensitive output:
  run both implementations against the same fixture path so the evidence is
  about behavior, not path drift.

## Native C++ integrity

Converted behavior only counts as native when the semantics live in `cpp/`.
Thin Python wrappers are allowed for object plumbing, imports, and unavoidable
runtime integration, but wrapper-heavy behavior remains uncounted.

To keep that boundary enforceable:

- `scripts/check_native_cpp.py --all` inventories embedded-Python execution and
  related pseudo-C++ patterns in `cpp/`.
- CI should block new occurrences in diffs under `cpp/`.
- Existing occurrences are native-debt until removed and should not be counted
  toward native-converted coverage.

## Recommended workflow

1. Verify the frozen manifest.
2. Rebuild the editable install after native changes.
3. Run the narrow differential parity lane.
4. Record or report the exact suite, fixture, and output form used as proof.
5. Only then run benchmarks or update conversion-coverage numbers.

## Reference sources

- GitHub Actions workflow artifacts:
  <https://docs.github.com/en/actions/managing-workflow-runs/downloading-workflow-artifacts>
- Hypothesis property-based testing docs:
  <https://hypothesis.readthedocs.io/>
- pyperf benchmark reproducibility and system guidance:
  <https://pyperf.readthedocs.io/>

# Immutable Parity Proof

Parity claims in this repository should leave behind evidence that can be
rechecked later without trusting memory or hand-written summaries.

## Frozen upstream immutability

- `source_code_v.0.15.0/` stays immutable as the reference oracle.
- `scripts/frozen_source_manifest.py` generates and verifies a committed
  SHA256 manifest for the frozen snapshot.
- The snapshot protection workflow should reject both direct Git edits to the
  frozen snapshot and manifest mismatches against the committed content hash.

Verify the local checkout before using the frozen snapshot as evidence:

```bash
.venv/bin/python scripts/frozen_source_manifest.py --verify
```

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

Converted behavior only counts as pure-native when the semantics live in
`cpp/native/`.
No active binding layer remains in current `main`. The optional `cpp/api/` C
ABI is allowed only as raw-buffer/JSON FFI over native semantics. Any
reintroduced Python-object binding glue would require an explicit temporary
exception, and mixed binding-plus-semantic files do not count as pure-native
conversion.

To keep that boundary enforceable:

- `scripts/check_native_cpp.py --all` inventories embedded-Python execution and
  related pseudo-C++ patterns in `cpp/`.
- `scripts/check_pure_native_cpp.py` enforces that `cpp/native/` stays free of
  Python integration tokens and reports or fails on mixed debt elsewhere.
- `docs/reference/native-cpp-debt-baseline.json` records the only allowed
  remaining mixed-debt footprint until it is migrated away; CI should fail if
  that debt grows or spreads.
- CI should block both new pseudo-C++ patterns and layout violations.
- Existing mixed files are native debt until split and should not be counted
  toward pure-native coverage.

## Recommended workflow

1. Verify the frozen manifest:

   ```bash
   .venv/bin/python scripts/frozen_source_manifest.py --verify
   ```

2. Rebuild the development harness and native targets that the proof will use:

   ```bash
   .venv/bin/python -m pip install -e '.[dev]' --no-build-isolation
   /usr/local/bin/cmake --build build-cpp -j2
   ```

3. Check that the converted surface still satisfies the native integrity
   boundary:

   ```bash
   .venv/bin/python scripts/check_native_cpp.py --all
   .venv/bin/python scripts/check_pure_native_cpp.py \
     --enforce-pure-native-subtree \
     --report-existing-debt \
     --baseline-json docs/reference/native-cpp-debt-baseline.json \
     --fail-on-baseline-regression
   ```

4. Run the narrowest differential parity lane for the touched surface. Prefer a
   focused test file or benchmark `--verify-only` command over an unrelated
   broad run:

   ```bash
   .venv/bin/python -m pytest -q tests/test_format_equivalence.py
   .venv/bin/python -m benchmarks.run --suite public-api --verify-only
   ```

5. Record the proof packet before making a claim: command lines, upstream
   snapshot manifest verification result, native build identifier or local build
   directory, suite or test names, fixture path or fixture identifier, and the
   output form compared.

6. Only then run benchmarks or update conversion-coverage numbers. Benchmark
   reports should keep the timing convention from
   `docs/reference/benchmark-suite.md`: Python time, native C++ time, time saved,
   reduction, and native C++ speedup over Python
   (`python_time / native_cpp_time`).

## Reference sources

- GitHub Actions workflow artifacts:
  <https://docs.github.com/en/actions/managing-workflow-runs/downloading-workflow-artifacts>
- Hypothesis property-based testing docs:
  <https://hypothesis.readthedocs.io/>
- pyperf benchmark reproducibility and system guidance:
  <https://pyperf.readthedocs.io/>

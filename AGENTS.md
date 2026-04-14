# AGENTS guide

This repository incrementally ports the frozen `ome/ome-zarr-py` `v0.15.0`
snapshot to C++ without modifying the snapshot itself.

## Primary goals

1. Preserve exact behavioral parity with the frozen Python upstream.
2. Maximize measurable performance gains once parity is already proven.

## Mandatory rules

1. Never edit `source_code_v.0.15.0/`.
2. Every converted surface must be checked against the frozen upstream code.
3. Preserve upstream exceptions, edge cases, and quirks unless an intentional
   divergence is explicitly documented.
4. Do not claim performance gains without benchmark data.
5. Use repo-local tooling in `.venv/` unless there is a hard blocker.
6. Use latest stable tool versions unless the repo pins something stricter.
7. Do not use background agents or subagents unless the user explicitly asks.
8. Before making any claim about branches, default branch, remotes, tags, or
   GitHub repo state, verify both local Git state and remote GitHub state.
9. After any push performed by an AI agent, wait for all repository workflows on
   the pushed commit to finish and treat the work as incomplete until they are
   green or the remaining failures are explicitly explained and queued for fix.
10. If package names, module paths, or repo naming change, remove stale build
    artifacts and editable-install metadata and rebuild before trusting tests.
11. If parity depends on Python runtime exception text, derive it from the live
    interpreter behavior instead of hard-coding strings that may vary by
    version.
12. Do not force-push to solve divergence on the default branch. Fetch, inspect
    the remote delta, then rebase or merge intentionally.
13. When using `gh` for Actions or remote repo checks, pin the target repo with
    `-R owner/repo` unless the active `gh` repo context has been explicitly
    verified.
14. After any C++ or extension-build change, reinstall the editable package
    before rerunning parity tests so the checks exercise the current binary.
15. In pybind11 ports, do not use `py::cast<bool>` as a generic replacement for
    Python truthiness on arbitrary objects such as lists or `None`. Use Python
    truth-value evaluation so `if obj:` behavior matches upstream.
16. For ports that mutate persistent stores or files, parity tests must compare
    serialized on-disk effects between upstream and the port, not just return
    values.
17. For ports that cross external boundaries such as browser launch, server
    startup, or similar side effects, parity tests must patch those boundaries
    and compare the resulting call payloads instead of triggering the real side
    effect.
18. Do not normalize, simplify, or otherwise "improve" upstream behavior during
    a parity port. Identical behavior is the default requirement unless an
    intentional divergence is explicitly documented.
19. Do not introduce `py::exec`, `py::eval`, or raw embedded-Python source
    blocks into `cpp/`. If historical embedded-Python debt is discovered,
    remove or isolate it before claiming native completion.
20. Security scanners in this repo must target repo-maintained code only.
    Frozen snapshot directories matching `source_code_v*/` must stay excluded
    from CodeQL and any future security scanning workflows.
21. If a new frozen snapshot directory is added or the snapshot naming changes,
    update the scanner exclusions in the same change before pushing.
22. Before claiming a workflow action or tooling pin is "latest stable", verify
    it against the official release or package index source instead of memory.
23. If repo-local skill coverage is missing for pybind11 runtime-parity work or
    workflow/dependency-governance work, add or update a repo-local skill with
    official references before relying on ad-hoc process notes.
24. Treat tests as first-class repo code. If CodeQL, lint, or security findings
    land in `tests/`, fix the underlying structure or logic instead of muting
    the finding because the code lives under `tests/`.
25. If multiple test modules share helpers, keep those helpers importable in
    both narrow invocations and whole-suite collection. Prefer an explicit
    `tests` package over fragile path-dependent imports.
26. If a parity surface prints or serializes absolute paths, run the upstream
    and converted implementations against the same fixture path whenever the
    surface is read-only so path text does not create false mismatches.
27. Keep `README.md` strictly user-facing. Agent instructions, workflow rules,
    and AI operating contracts belong in `AGENTS.md`, `.github/instructions/`,
    and repo-local skills only.
28. Before making or repeating a frozen-snapshot immutability claim, verify the
    committed SHA256 manifest with
    `.venv/bin/python scripts/frozen_source_manifest.py --verify`.
29. If a change touches `cpp/`, scan it with
    `.venv/bin/python scripts/check_native_cpp.py --all` and treat any
    embedded-Python execution pattern in `cpp/` as native-debt rather than
    fully converted C++ coverage.
30. The pure-native C++ policy applies to the whole repository, including
    existing converted code. `cpp/native/` is for real C++ semantics only,
    `cpp/bindings/` is for minimal boundary glue only, and mixed files
    elsewhere under `cpp/` are remediation debt.
31. Do not count any converted surface as pure-native unless its semantics live
    in `cpp/native/`. "Native-backed" and "pure-native" are different claims.
32. If a change touches `cpp/`, run
    `.venv/bin/python scripts/check_pure_native_cpp.py --enforce-pure-native-subtree --report-existing-debt`
    and treat any Python-integration tokens outside `cpp/bindings/` as a hard
    architectural problem to be removed, not documented away.
33. Python objects are not allowed in C++ semantic code. The only acceptable
    exception is a boundary shim in `cpp/bindings/` when it can be shown with
    high confidence that no practical native alternative exists for preserving
    the Python-visible contract.
34. The rule against Python-object semantics applies retroactively to existing
    converted code as well as new work. Existing violations are remediation
    debt and must not be disguised as completed native conversion.

## Fast load order

1. `docs/reference/architecture-first-porting.md`
2. `docs/reference/pure-native-cpp-policy.md`
3. `docs/reference/ai-agent-context-routing.md`
4. `docs/reference/porting-contract.md`
5. `docs/reference/ai-agent-dos-and-donts.md`
6. `docs/reference/immutable-parity-proof.md`
7. `docs/reference/ai-agent-skills.md`
8. the touched upstream implementation file under `source_code_v.0.15.0/`
9. the matching wrapper, C++ file, and nearest test module

## Repository map

- `source_code_v.0.15.0/`: immutable upstream reference snapshot
- `cpp/`: C++ implementations
- `cpp/native/`: pure-native semantics only
- `cpp/bindings/`: minimal Python boundary glue only
- `ome_zarr_c/`: Python compatibility wrappers
- `tests/`: parity and regression tests
- `docs/`: repo rules, routing, and benchmarks
- `.agents/skills/`: reusable repo-local workflows

## Verification minimum

```bash
.venv/bin/python scripts/frozen_source_manifest.py --verify
.venv/bin/python -m pip install -e . --no-build-isolation
timeout 180s .venv/bin/python -m pytest -q \
  tests/test_axes_equivalence.py \
  tests/test_conversions_equivalence.py \
  tests/test_dask_utils_equivalence.py \
  tests/test_data_equivalence.py \
  tests/test_format_equivalence.py \
  tests/test_reader_equivalence.py \
  tests/test_scale_equivalence.py \
  tests/test_scaler_equivalence.py \
  tests/test_utils_equivalence.py \
  tests/test_writer_equivalence.py
.venv/bin/python scripts/check_native_cpp.py --all
.venv/bin/python scripts/check_pure_native_cpp.py --enforce-pure-native-subtree --report-existing-debt
.venv/bin/python -m ruff check .
.venv/bin/python -m ruff format --check .
```

Run only the narrowest relevant suites while iterating, but report exactly what
was verified. Do not claim blocked store-backed lanes are green by implication.

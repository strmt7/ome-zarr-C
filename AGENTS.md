# AGENTS guide

## Professional presentation rule

Do not expose internal reasoning, private discussions, transient process notes,
or implementation history in user-facing docs, release material, examples, or
commit/PR text. Public-facing material must read as professional project
documentation: factual, concise, supportable, and focused on verified product
behavior.

## Critical single-session rule

Do not spawn, delegate to, or coordinate with multiple AI agents, subagents, or
separate agent sessions. All AI work in this repository must stay in one
session unless the user explicitly revokes this rule in a later instruction.
Never bypass, weaken, or disregard this rule to save time or run work
concurrently.

If any AI-agent work is already in progress when this rule is read, let that
work finish fully, then harvest and merge its outputs without losing changes
before continuing in a single session.

This repository incrementally ports the frozen `ome/ome-zarr-py` `v0.15.0`
snapshot to C++ without modifying the snapshot itself.

Current `main` uses Python only as a development oracle for parity tests,
fixture generation, and benchmark comparison. The intended product path is the
standalone C++ implementation under `cpp/native/`, `cpp/api/`, and
`cpp/tools/`; no repo-maintained Python compatibility package is active.

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
6. Use the latest stable tool and dependency versions when the repo does not
   pin something stricter, but only keep an upgrade if exact parity is still
   proven on the upgraded stack.
7. Do not spawn, delegate to, or coordinate with multiple AI agents, subagents,
   or separate agent sessions. Work in one session only unless the user
   explicitly revokes the critical single-session rule above in a later
   instruction.
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
14. After any C++ or package-build change, rebuild the native targets or
    reinstall the editable package before rerunning parity tests so the checks
    exercise the current files.
15. Do not reintroduce pybind11 ports. If historical pybind code is found,
    remove it or keep it only as already-failing debt until it can be deleted;
    do not add new `py::` objects to preserve behavior.
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
23. Newer stable dependency versions are allowed and often desirable when they
    unlock better implementation techniques or performance, but every such
    upgrade must be followed by parity revalidation on the real runtime before
    it is treated as acceptable.
24. If repo-local skill coverage is missing for standalone C++ parity work or
    workflow/dependency-governance work, add or update a repo-local skill with
    official references before relying on ad-hoc process notes.
25. Treat tests as first-class repo code. If CodeQL, lint, or security findings
    land in `tests/`, fix the underlying structure or logic instead of muting
    the finding because the code lives under `tests/`.
26. If multiple test modules share helpers, keep those helpers importable in
    both narrow invocations and whole-suite collection. Prefer an explicit
    `tests` package over fragile path-dependent imports.
27. If a parity surface prints or serializes absolute paths, run the upstream
    and converted implementations against the same fixture path whenever the
    surface is read-only so path text does not create false mismatches.
28. Keep `README.md` strictly user-facing. Internal automation instructions,
    workflow rules, and operating contracts belong in `AGENTS.md`,
    `.github/instructions/`, and repo-local skills only.
29. Before making or repeating a frozen-snapshot immutability claim, verify the
    committed SHA256 manifest with
    `.venv/bin/python scripts/frozen_source_manifest.py --verify`.
30. If a change touches `cpp/`, scan it with
    `.venv/bin/python scripts/check_native_cpp.py --all` and treat any
    embedded-Python execution pattern in `cpp/` as native-debt rather than
    fully converted C++ coverage.
31. The pure-native C++ policy applies to the whole repository, including
    existing converted code. `cpp/native/` is for real C++ semantics only,
    `cpp/tools/` may expose standalone native entrypoints, and no active
    binding layer remains.
32. Do not count any converted surface as pure-native unless its semantics live
    in `cpp/native/`. Compiled-extension-backed and `pure-native` are
    different claims.
33. If a change touches `cpp/`, run
    `.venv/bin/python scripts/check_pure_native_cpp.py --enforce-pure-native-subtree --report-existing-debt`
    and treat any Python-integration tokens under `cpp/` as a hard
    architectural problem to be removed, not documented away.
34. Python objects are not allowed in C++ semantic code. The only acceptable
    exception is a deliberately approved, temporary boundary shim when it can
    be shown with high confidence that no practical native alternative exists
    for preserving parity. No such active shim exists in current `main`.
35. The rule against Python-object semantics applies retroactively to existing
    converted code as well as new work. Existing violations are remediation
    debt and must not be disguised as completed native conversion.
36. Ruff is Python-only in this repository. Never point Ruff at `cpp/` or any
    C/C++ file extension such as `*.cpp`, `*.hpp`, `*.cc`, `*.cxx`, or `*.h`.
    If linting a subset, pass only Python or Markdown-like paths explicitly.
    Use native checks, compiler/test validation, and C++-appropriate tooling
    for C++ files instead.
37. During local performance work, benchmark only the touched surface first by
    using narrow `--suite`, `--group`, and `--match` filters together with
    `--fast`, and keep exploratory runs under an explicit timeout. Do not kick
    off broad multi-minute benchmark reruns unless they are needed for the
    final performance claim or a deliberate benchmark artifact refresh.
38. Before any push, perform a smart repo-wide consistency scan over the
    touched surface and adjacent contract files. That scan must look for stale
    or conflicting docs, mismatched file/function/module names, outdated
    references to removed structures, benchmark or coverage claims that no
    longer match committed artifacts, and similar internal drift. Do not treat
    passing tests alone as proof that the repo text and structure are aligned.
39. Use selective context loading on purpose. Start with the smallest
    sufficient set of docs, code, tests, and skills needed to avoid guessing,
    then broaden only when evidence says the first pass is not enough. Saving
    tokens is not the goal by itself; the goal is to keep context high-signal
    without making assumptions.
40. Treat GitHub `main` as the repository source of truth for shared state.
    Local branches and local artifacts are disposable until their content is
    reconciled with `origin/main`.
41. Do not expand the Python runtime footprint of the shipped product. Python
    package metadata is development harness infrastructure only and must not
    be mistaken for the intended final delivery shape. pybind bindings and
    repo-maintained Python compatibility packages are not part of the current
    architecture.
42. When working on performance-sensitive native code, separate boundary
    overhead from core semantics. Measure frozen-upstream Python separately
    from standalone native C++; do not introduce Python package-path converted
    timings.
43. Do not expose plan-only or helper-only commands in the shipped standalone
    native CLI. Native CLI surfaces must correspond to real runtime commands
    or durable product APIs, not temporary inspection shortcuts.
44. When replacing runtime behavior, extend the standalone native library or
    CLI first. Do not create a repo-maintained Python compatibility package as
    benchmark glue or product scaffolding.
45. Once a standalone-native runtime command or library entrypoint exists and
    parity is proven for that surface, delete corresponding Python-harness
    runtime scaffolding unless it is strictly frozen-upstream oracle code.
46. For runtime surfaces already replaced by the standalone native product,
    delete obsolete layers instead of forwarding calls through them. Remove
    stale benchmark references and obsolete runtime tests in the same slice
    unless a remaining oracle-only dependency can be stated precisely and
    verified.
47. If parity proof and performance measurement for a surface can be carried by
    standalone native tools plus the frozen Python oracle, do not keep a pybind
    or Python package version of that surface alive just to satisfy old tests
    or benchmarks. Repoint the tests/benchmarks and delete the old layer.
48. For the standalone native toolchain, use the pinned latest-version manifest
    in `docs/reference/native-dependency-manifest.json` as the source of truth.
    Do not rely on stale distro package versions for CMake, Ninja, Zstd, or
    c-blosc when the manifest specifies newer releases.
49. Report pure-native benchmark outcomes in time terms first: Python time,
    native C++ time, time saved per operation, and native C++ time reduction.
    If a ratio is included, label it as native C++ speedup over Python:
    `python_time / native_cpp_time`. A ratio above `1.0x` means native C++ is
    faster; below `1.0x` means native C++ is slower. Do not use shorthand
    labels that omit the ratio direction, and do not invert slower cases into
    larger "slower" multipliers; report the direct speedup ratio, for example
    `0.748x`.
50. Never call a Python package-path timing "C++" or "native C++". Native C++
    benchmark claims require standalone native C++ executable/library timing.
51. Keep all correctness tests in the root `tests/` folder. Do not hide tests
    in benchmark subdirectories.
52. Keep all timing and benchmark orchestration under `benchmarks/`. Python
    upstream timing helpers belong under `benchmarks/python/`; standalone
    native C++ timing helpers belong under `benchmarks/native/`.
53. Optional external interoperability belongs in `cpp/api/` as a C ABI over
    `cpp/native/` semantics. It may expose raw buffers and JSON strings for
    `ctypes`, CFFI, C, C++, Rust, Julia, or similar callers, but it must not
    include Python objects, pybind glue, CPython headers, embedded interpreter
    calls, or package-specific Python adapter logic.
54. For ABI work, test at least one real external package boundary when it is
    relevant. NumPy-style array entrypoints must be tested with real contiguous
    NumPy memory through `ctypes`; local store/path entrypoints must be tested
    against frozen-upstream behavior on real Zarr-created stores when possible.

## Fast load order

1. `docs/reference/architecture-first-porting.md`
2. `docs/reference/standalone-cpp-target.md`
3. `docs/reference/native-build-and-selftest.md`
4. `docs/reference/native-c-api-interop.md`
5. `docs/reference/native-dependency-manifest.json`
6. `docs/reference/pure-native-cpp-policy.md`
7. `docs/reference/ai-agent-context-routing.md`
8. `docs/reference/porting-contract.md`
9. `docs/reference/ai-agent-dos-and-donts.md`
10. `docs/reference/immutable-parity-proof.md`
11. `docs/reference/ai-agent-skills.md`
12. the touched upstream implementation file under `source_code_v.0.15.0/`
13. the matching C++ file, benchmark helper, and nearest test module

## Repository map

- `source_code_v.0.15.0/`: immutable upstream reference snapshot
- `cpp/`: C++ implementations
- `cpp/native/`: pure-native semantics only
- `cpp/api/`: optional C ABI interoperability boundary over native semantics
- `cpp/tools/`: standalone native CLI, self-test, and bounded native benchmarks
- `tests/`: parity and regression tests
- `benchmarks/`: Python-upstream and standalone-native benchmark code
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
  tests/test_native_c_api_interop.py \
  tests/test_utils_equivalence.py \
  tests/test_writer_equivalence.py
.venv/bin/python scripts/check_native_cpp.py --all
.venv/bin/python scripts/check_pure_native_cpp.py --enforce-pure-native-subtree --report-existing-debt
.venv/bin/python scripts/check_repo_consistency.py
.venv/bin/python -m ruff check .
.venv/bin/python -m ruff format --check .
./scripts/install_latest_native_toolchain.sh /usr/local
/usr/local/bin/cmake -S . -B /tmp/ome-zarr-c-cmake -G Ninja -DCMAKE_BUILD_TYPE=Release
/usr/local/bin/cmake --build /tmp/ome-zarr-c-cmake -j2
ctest --test-dir /tmp/ome-zarr-c-cmake --output-on-failure
```

Run only the narrowest relevant suites while iterating, but report exactly what
was verified. If running Ruff on a subset, pass only Python or Markdown-like
paths explicitly and never include C++ files. Do not claim blocked
store-backed lanes are green by implication.

# Pure-Native C++ Policy

This policy applies to the entire repository, including existing converted
code, not only future additions.

## Target structure

- `cpp/native/`
  real C++ semantics only
- `cpp/api/`
  optional C ABI boundary over `cpp/native/` semantics only
- `cpp/tools/`
  standalone native executable entrypoints only
- `cpp/assets/`
  deterministic native fixture assets generated from verified source data
- any other file under `cpp/`
  transitional debt that must be removed or relocated

## Hard rules

- Code in `cpp/native/` must not contain `py::`, `pybind11` headers,
  Python C-API calls, or Python attribute dispatch.
- Python objects are forbidden in C++ semantic code. The optional C ABI may be
  used by external Python packages through `ctypes`, CFFI, or similar FFI
  loaders, but C++ code must not include CPython objects, pybind dispatch, or
  package-specific Python semantics.
- A mixed file that combines binding glue and business logic does not count as
  pure-native, even if it compiles and passes parity tests.
- Existing mixed files are subject to the same rule. They are debt, not exempt.
- Performance work does not relax the rule. Faster code still has to preserve
  exact parity and keep the semantics native.

## Coverage language

- `pure-native` means the semantics live in `cpp/native/`.
- `c-abi-interop` means a thin C ABI forwards external callers into
  `cpp/native/` semantics and uses raw buffers or JSON only.
- `compiled-extension-backed` means compiled extension code participates, but
  Python-facing objects or mixed binding/logic code still carry part of the
  semantics.
- Do not present compiled-extension-backed coverage as `pure-native` coverage.

## Enforcement

- `scripts/check_native_cpp.py` blocks embedded-Python execution patterns such
  as `py::exec` and `py::eval`.
- `scripts/check_pure_native_cpp.py` enforces that `cpp/native/` stays free of
  Python integration code and reports or fails on mixed debt elsewhere in
  `cpp/`.
- `cpp/api/` is acceptable only while it remains C ABI glue over native
  semantics. If it grows Python-object dispatch, it becomes architectural
  debt, not product API.
- `docs/reference/native-cpp-debt-baseline.json` ratchets the remaining mixed
  debt so it can only decrease and cannot spread to new files.

## Migration rule

Until a surface has its semantics in `cpp/native/` and its runtime path can be
exercised without Python binding glue, that surface remains in remediation and
should not be counted as pure-native conversion progress.

## Priority order

1. Match the frozen Python upstream exactly.
2. Increase performance only after parity is proven and without moving
   semantics back toward Python objects.

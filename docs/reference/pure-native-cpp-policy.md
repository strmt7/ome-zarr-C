# Pure-Native C++ Policy

This policy applies to the entire repository, including existing converted
code, not only future additions.

## Target structure

- `cpp/native/`
  real C++ semantics only
- `cpp/tools/`
  standalone native executable entrypoints only
- any other file under `cpp/`
  transitional debt that must be removed or relocated

## Hard rules

- Code in `cpp/native/` must not contain `py::`, `pybind11` headers,
  Python C-API calls, or Python attribute dispatch.
- Python objects are forbidden in C++ semantic code. Python interop is not part
  of the current C++ architecture and must not be reintroduced without a
  documented, temporary, unavoidable exception.
- A mixed file that combines binding glue and business logic does not count as
  pure-native, even if it compiles and passes parity tests.
- Existing mixed files are subject to the same rule. They are debt, not exempt.
- Performance work does not relax the rule. Faster code still has to preserve
  exact parity and keep the semantics native.

## Coverage language

- `pure-native` means the semantics live in `cpp/native/`.
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

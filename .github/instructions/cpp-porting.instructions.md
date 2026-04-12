# C++ Porting Instructions

- Do not modify the frozen upstream snapshot under `source_code_v.0.15.0/`.
- Port one small upstream surface at a time.
- Keep Python wrappers as thin compatibility layers.
- Preserve upstream exceptions and edge-case behavior unless an intentional
  divergence is explicitly documented.
- If upstream behavior depends on interpreter-generated exception text, use the
  live Python runtime to produce that behavior instead of freezing a message
  literal in C++.
- Do not use `py::cast<bool>` for generic Python truthiness on arbitrary
  objects. Match Python `if obj:` semantics with Python truth-value evaluation.
- After changing native code or extension build surfaces, rebuild the editable
  install before rerunning parity tests.
- Add differential tests against the frozen snapshot before claiming parity.
- Benchmark before claiming any performance improvement.

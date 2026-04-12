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
- For functions that mutate files or stores, compare the resulting serialized
  on-disk state against upstream instead of only checking return values.
- For functions that cross external side-effect boundaries, patch the boundary
  and compare the outbound call payloads against upstream instead of triggering
  the real side effect.
- Do not normalize or "improve" upstream behavior for convenience. Parity means
  preserving the same behavior unless a divergence is explicitly documented.
- If `py::exec` defines Python classes whose methods reference runtime names,
  use a shared scope for globals and locals so those names remain available
  when the methods execute later.
- After changing native code or extension build surfaces, rebuild the editable
  install before rerunning parity tests.
- Add differential tests against the frozen snapshot before claiming parity.
- Benchmark before claiming any performance improvement.

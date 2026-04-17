# Testing Instructions

Critical: do not spawn, delegate to, or coordinate with multiple AI agents,
subagents, or separate agent sessions. Work in one session only unless the user
explicitly revokes this rule in a later instruction.

Public-facing repository material must not expose internal reasoning, private
discussions, transient process notes.

- Run parity suites separately instead of batching unrelated converted surfaces
  into one giant test command.
- Prefer exhaustive differential tests when the state space is bounded.
- For unbounded inputs, use boundary cases, randomized differential tests, and
  representative real-data checks.
- Never claim broader parity or coverage than the executed tests actually prove.
- When the proof depends on the frozen upstream snapshot, verify the committed
  SHA256 manifest instead of assuming the tree stayed pristine.
- When workflow or security-scan scope changes are made, verify the actual
  scanner config and not just the workflow trigger stanza.
- Treat test code as production-quality repo code. Fix lint, CodeQL, and
  security findings in `tests/` at the root cause instead of suppressing them.
- Shared test helpers must work in both focused test runs and whole-suite
  collection. Keep them under an importable `tests` package instead of relying
  on accidental import-path behavior.
- If a read-only surface emits absolute paths as observable output, run the
  upstream and converted implementations against the same fixture path so the
  parity check measures behavior instead of fixture-location drift.
- If `cpp/` changes are part of the claim, distinguish truly native behavior
  from embedded-Python debt inside `cpp/`.

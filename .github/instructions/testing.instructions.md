# Testing Instructions

- Run parity suites separately instead of batching unrelated converted surfaces
  into one giant test command.
- Prefer exhaustive differential tests when the state space is bounded.
- For unbounded inputs, use boundary cases, randomized differential tests, and
  representative real-data checks.
- Never claim broader parity or coverage than the executed tests actually prove.

# Security Policy

## Scope

This repository contains:

- an immutable upstream reference snapshot under `source_code_v.0.15.0/`
- new C++ and Python compatibility code outside that snapshot

Security reports should focus on the repo-maintained code and workflow surfaces
outside the frozen upstream snapshot unless the issue is specifically about how
the snapshot is imported, preserved, or validated.

The CodeQL configuration excludes frozen snapshot directories matching
`source_code_v*/**`. If another frozen snapshot is added in the future, keep
that exclusion aligned in the same change so security alerts stay focused on
repo-maintained code.

## Reporting

Open a private security report through GitHub Security Advisories if available,
or contact the repository owner directly. Do not open a public issue for active
credential exposure or exploitable workflow flaws.

## Expectations

- frozen upstream code is preserved for provenance, not actively developed here
- all newly added code and workflows should follow least-privilege defaults
- benchmark fixtures and examples must not contain secrets or proprietary data

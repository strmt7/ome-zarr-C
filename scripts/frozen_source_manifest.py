#!/usr/bin/env python3
"""Generate or verify the frozen-source SHA256 manifest."""

from __future__ import annotations

import argparse
import hashlib
import sys
from pathlib import Path

DEFAULT_ROOT = Path("source_code_v.0.15.0")
DEFAULT_MANIFEST = Path("docs/reference/frozen-source-manifest.sha256")
IGNORED_NAMES = {
    ".pytest_cache",
    ".ruff_cache",
    "__pycache__",
}
IGNORED_SUFFIXES = {".pyc", ".pyo"}


def iter_files(root: Path):
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        if any(part in IGNORED_NAMES for part in path.parts):
            continue
        if path.suffix in IGNORED_SUFFIXES:
            continue
        yield path


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_manifest(root: Path) -> str:
    lines = []
    for path in iter_files(root):
        relpath = path.relative_to(root).as_posix()
        lines.append(f"{file_sha256(path)}  {relpath}")
    return "\n".join(lines) + "\n"


def parse_manifest(text: str) -> dict[str, str]:
    entries: dict[str, str] = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        digest, relpath = line.split("  ", 1)
        entries[relpath] = digest
    return entries


def verify_manifest(root: Path, manifest_path: Path) -> int:
    expected = parse_manifest(manifest_path.read_text())
    actual = parse_manifest(build_manifest(root))

    changed = sorted(
        path for path in actual if path in expected and actual[path] != expected[path]
    )
    missing = sorted(path for path in expected if path not in actual)
    extra = sorted(path for path in actual if path not in expected)

    if not changed and not missing and not extra:
        print(f"Manifest verified for {root}")
        return 0

    if changed:
        print("Changed files:")
        for path in changed:
            print(f"  {path}")
    if missing:
        print("Missing files:")
        for path in missing:
            print(f"  {path}")
    if extra:
        print("Extra files:")
        for path in extra:
            print(f"  {path}")
    return 1


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument(
        "--verify",
        action="store_true",
        help="verify the current frozen source tree against the manifest",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="write the generated manifest to --manifest instead of stdout",
    )
    args = parser.parse_args()

    if args.verify:
        if not args.manifest.exists():
            print(f"Manifest does not exist: {args.manifest}", file=sys.stderr)
            return 1
        return verify_manifest(args.root, args.manifest)

    manifest = build_manifest(args.root)
    if args.write:
        args.manifest.parent.mkdir(parents=True, exist_ok=True)
        args.manifest.write_text(manifest)
        print(f"Wrote manifest to {args.manifest}")
        return 0

    sys.stdout.write(manifest)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

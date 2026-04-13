from __future__ import annotations

import json
import os
import shutil
import urllib.request
import zipfile
from dataclasses import asdict, dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_FIXTURE_ROOT = ROOT / ".benchmarks-fixtures"


@dataclass(frozen=True)
class FixtureSpec:
    dataset_id: str
    archive_url: str
    source_url: str
    license_name: str
    size_label: str
    description: str
    tier: str
    archive_kind: str
    inner_glob: str | None = None
    inner_path: str | None = None


EXAMPLES_COMMIT = "8c10c88fbb77c3dcc206d9b234431f243beee576"

_SPECS = {
    "examples_image": FixtureSpec(
        dataset_id="examples_image",
        archive_url=(
            "https://codeload.github.com/BioImageTools/ome-zarr-examples/zip/"
            f"{EXAMPLES_COMMIT}"
        ),
        source_url="https://github.com/BioImageTools/ome-zarr-examples",
        license_name="BSD-3-Clause",
        size_label="repository archive",
        description="Small valid OME-Zarr image fixture from ome-zarr-examples.",
        tier="small",
        archive_kind="zip",
        inner_path="data/valid/image-03.zarr",
    ),
    "examples_plate": FixtureSpec(
        dataset_id="examples_plate",
        archive_url=(
            "https://codeload.github.com/BioImageTools/ome-zarr-examples/zip/"
            f"{EXAMPLES_COMMIT}"
        ),
        source_url="https://github.com/BioImageTools/ome-zarr-examples",
        license_name="BSD-3-Clause",
        size_label="repository archive",
        description="Small valid OME-Zarr plate fixture from ome-zarr-examples.",
        tier="small",
        archive_kind="zip",
        inner_path="data/valid/plate-01.zarr",
    ),
    "bia_tonsil3": FixtureSpec(
        dataset_id="bia_tonsil3",
        archive_url=(
            "https://ftp.ebi.ac.uk/biostudies/fire/S-BIAD/800/S-BIAD800/Files/"
            "idr0054/Tonsil%203.ome.zarr.zip"
        ),
        source_url=(
            "https://uk1s3.embassy.ebi.ac.uk/bia-integrator-data/pages/"
            "S-BIAD800/f49ada41-43bf-47ff-99b9-bdf8cc311ce3.html"
        ),
        license_name="CC BY 4.0",
        size_label="108.8 MiB",
        description=(
            "Public OME-NGFF imaging mass cytometry tonsil section from the "
            "BioImage Archive / IDR."
        ),
        tier="medium",
        archive_kind="zip",
        inner_glob="*.ome.zarr",
    ),
    "bia_156_42": FixtureSpec(
        dataset_id="bia_156_42",
        archive_url=(
            "https://ftp.ebi.ac.uk/biostudies/fire/S-BIAD/885/S-BIAD885/Files/"
            "idr0010/156-42.ome.zarr.zip"
        ),
        source_url=(
            "https://uk1s3.embassy.ebi.ac.uk/bia-integrator-data/pages/"
            "S-BIAD885/a1cfd35d-81c5-448a-a368-2350eca76c6f.html"
        ),
        license_name="CC BY 4.0",
        size_label="455.3 MiB",
        description=(
            "Larger public OME-NGFF high-content screening image from the "
            "BioImage Archive / IDR."
        ),
        tier="large",
        archive_kind="zip",
        inner_glob="*.ome.zarr",
    ),
}


def resolved_fixture_root() -> Path:
    value = Path(
        os.environ.get("OME_ZARR_BENCH_FIXTURE_ROOT", str(DEFAULT_FIXTURE_ROOT))
    )
    value.mkdir(parents=True, exist_ok=True)
    return value


def fixture_specs() -> dict[str, FixtureSpec]:
    return dict(_SPECS)


def fixture_report() -> list[dict[str, str]]:
    return [asdict(spec) for spec in _SPECS.values()]


def _download(url: str, destination: Path) -> None:
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "ome-zarr-c benchmark fixture downloader"},
    )
    with urllib.request.urlopen(request) as response, destination.open("wb") as handle:
        shutil.copyfileobj(response, handle)


def _manifest_path(dataset_root: Path) -> Path:
    return dataset_root / "fixture.json"


def _extract_archive(archive_path: Path, extract_root: Path) -> None:
    if extract_root.exists():
        shutil.rmtree(extract_root)
    extract_root.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive_path) as archive:
        archive.extractall(extract_root)


def _locate_dataset_path(spec: FixtureSpec, extracted_root: Path) -> Path:
    if spec.inner_path is not None:
        matches = list(extracted_root.glob(f"*/{spec.inner_path}"))
        if len(matches) != 1:
            raise FileNotFoundError(
                f"expected exactly one extracted path for {spec.dataset_id}: "
                f"{spec.inner_path!r}"
            )
        return matches[0]

    if spec.inner_glob is not None:
        matches = sorted(extracted_root.rglob(spec.inner_glob))
        if len(matches) != 1:
            raise FileNotFoundError(
                f"expected exactly one extracted path for {spec.dataset_id}: "
                f"{spec.inner_glob!r}, found {len(matches)}"
            )
        return matches[0]

    raise ValueError(f"fixture {spec.dataset_id} is missing a dataset locator")


def ensure_fixture(dataset_id: str) -> Path:
    if dataset_id not in _SPECS:
        raise KeyError(f"unknown fixture dataset_id: {dataset_id}")

    spec = _SPECS[dataset_id]
    root = resolved_fixture_root()
    dataset_root = root / dataset_id
    archive_path = dataset_root / "archive.zip"
    extracted_root = dataset_root / "extracted"
    manifest_path = _manifest_path(dataset_root)

    dataset_root.mkdir(parents=True, exist_ok=True)
    if not archive_path.exists():
        _download(spec.archive_url, archive_path)
    if not extracted_root.exists():
        _extract_archive(archive_path, extracted_root)

    dataset_path = _locate_dataset_path(spec, extracted_root)
    manifest_path.write_text(
        json.dumps(
            {
                **asdict(spec),
                "dataset_path": str(dataset_path),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return dataset_path


def ensure_fixture_set(dataset_ids: list[str]) -> dict[str, Path]:
    return {dataset_id: ensure_fixture(dataset_id) for dataset_id in dataset_ids}


__all__ = [
    "FixtureSpec",
    "DEFAULT_FIXTURE_ROOT",
    "ensure_fixture",
    "ensure_fixture_set",
    "fixture_report",
    "fixture_specs",
    "resolved_fixture_root",
]

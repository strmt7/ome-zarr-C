#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MANIFEST_PATH="${ROOT_DIR}/docs/reference/native-dependency-manifest.json"
PREFIX="${1:-/usr/local}"
WORK_DIR="${ROOT_DIR}/.native-toolchain"
SRC_DIR="${WORK_DIR}/src"
BUILD_DIR="${WORK_DIR}/build"
DOWNLOAD_DIR="${WORK_DIR}/downloads"

manifest_read() {
  local json_path="$1"
  python3 - "${MANIFEST_PATH}" "${json_path}" <<'PY'
import json
import sys

manifest_path = sys.argv[1]
json_path = sys.argv[2].split(".")
with open(manifest_path, encoding="utf-8") as handle:
    payload = json.load(handle)
value = payload
for key in json_path:
    value = value[key]
print(value)
PY
}

HOST_OS="$(uname -s)"
HOST_ARCH="$(uname -m)"
if [[ "${HOST_OS}" != "Linux" ]]; then
  echo "ERROR: this installer currently supports Linux hosts only" >&2
  exit 1
fi
case "${HOST_ARCH}" in
  x86_64)
    CMAKE_ARCH="x86_64"
    NINJA_ASSET="ninja-linux.zip"
    ;;
  aarch64|arm64)
    CMAKE_ARCH="aarch64"
    NINJA_ASSET="ninja-linux-aarch64.zip"
    ;;
  *)
    echo "ERROR: unsupported Linux architecture: ${HOST_ARCH}" >&2
    exit 1
    ;;
esac

mkdir -p "${SRC_DIR}" "${BUILD_DIR}" "${DOWNLOAD_DIR}" "${PREFIX}/bin" "${PREFIX}/share" "${PREFIX}/doc"

CMAKE_VERSION="$(manifest_read host_tools.cmake.version)"
NINJA_VERSION="$(manifest_read host_tools.ninja.version)"
ZSTD_VERSION="$(manifest_read host_native_libs.zstd.version)"
ZLIB_VERSION="$(manifest_read host_native_libs.zlib.version)"
LZ4_VERSION="$(manifest_read host_native_libs.lz4.version)"
CBLOSC_VERSION="$(manifest_read host_native_libs.c-blosc.version)"

CMAKE_TARBALL="cmake-${CMAKE_VERSION}-linux-${CMAKE_ARCH}.tar.gz"
CMAKE_URL="https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/${CMAKE_TARBALL}"
NINJA_URL="https://github.com/ninja-build/ninja/releases/download/v${NINJA_VERSION}/${NINJA_ASSET}"
ZSTD_URL="https://github.com/facebook/zstd/archive/refs/tags/v${ZSTD_VERSION}.tar.gz"
ZLIB_URL="https://github.com/madler/zlib/archive/refs/tags/v${ZLIB_VERSION}.tar.gz"
LZ4_URL="https://github.com/lz4/lz4/archive/refs/tags/v${LZ4_VERSION}.tar.gz"
CBLOSC_URL="https://github.com/Blosc/c-blosc/archive/refs/tags/v${CBLOSC_VERSION}.tar.gz"

download_if_missing() {
  local url="$1"
  local output_path="$2"
  if [[ ! -f "${output_path}" ]]; then
    curl -L --fail --retry 3 --output "${output_path}" "${url}"
  fi
}

download_if_missing "${CMAKE_URL}" "${DOWNLOAD_DIR}/${CMAKE_TARBALL}"
download_if_missing "${NINJA_URL}" "${DOWNLOAD_DIR}/${NINJA_ASSET}"
download_if_missing "${ZSTD_URL}" "${DOWNLOAD_DIR}/zstd-${ZSTD_VERSION}.tar.gz"
download_if_missing "${ZLIB_URL}" "${DOWNLOAD_DIR}/zlib-${ZLIB_VERSION}.tar.gz"
download_if_missing "${LZ4_URL}" "${DOWNLOAD_DIR}/lz4-${LZ4_VERSION}.tar.gz"
download_if_missing "${CBLOSC_URL}" "${DOWNLOAD_DIR}/c-blosc-${CBLOSC_VERSION}.tar.gz"

rm -rf "${WORK_DIR}/cmake-extract"
mkdir -p "${WORK_DIR}/cmake-extract"
tar -xzf "${DOWNLOAD_DIR}/${CMAKE_TARBALL}" -C "${WORK_DIR}/cmake-extract"
CMAKE_EXTRACT_DIR="$(find "${WORK_DIR}/cmake-extract" -mindepth 1 -maxdepth 1 -type d | head -n 1)"
cp -a "${CMAKE_EXTRACT_DIR}/bin/." "${PREFIX}/bin/"
cp -a "${CMAKE_EXTRACT_DIR}/share/." "${PREFIX}/share/"
cp -a "${CMAKE_EXTRACT_DIR}/doc/." "${PREFIX}/doc/"
if [[ -d "${CMAKE_EXTRACT_DIR}/man" ]]; then
  mkdir -p "${PREFIX}/man"
  cp -a "${CMAKE_EXTRACT_DIR}/man/." "${PREFIX}/man/"
fi

python3 - "${DOWNLOAD_DIR}/${NINJA_ASSET}" "${PREFIX}/bin/ninja" <<'PY'
import os
import stat
import sys
import zipfile

archive_path = sys.argv[1]
target_path = sys.argv[2]
with zipfile.ZipFile(archive_path) as archive:
    member = next(name for name in archive.namelist() if name.endswith("/ninja") or name == "ninja")
    with archive.open(member) as source, open(target_path, "wb") as target:
        target.write(source.read())
mode = os.stat(target_path).st_mode
os.chmod(target_path, mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
PY

export PATH="${PREFIX}/bin:${PATH}"

rm -rf "${SRC_DIR}/zstd-${ZSTD_VERSION}" "${BUILD_DIR}/zstd-${ZSTD_VERSION}"
tar -xzf "${DOWNLOAD_DIR}/zstd-${ZSTD_VERSION}.tar.gz" -C "${SRC_DIR}"
"${PREFIX}/bin/cmake" \
  -S "${SRC_DIR}/zstd-${ZSTD_VERSION}/build/cmake" \
  -B "${BUILD_DIR}/zstd-${ZSTD_VERSION}" \
  -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX="${PREFIX}" \
  -DZSTD_BUILD_PROGRAMS=OFF \
  -DZSTD_BUILD_TESTS=OFF
"${PREFIX}/bin/cmake" --build "${BUILD_DIR}/zstd-${ZSTD_VERSION}" -j2
"${PREFIX}/bin/cmake" --install "${BUILD_DIR}/zstd-${ZSTD_VERSION}"

rm -rf "${SRC_DIR}/zlib-${ZLIB_VERSION}" "${BUILD_DIR}/zlib-${ZLIB_VERSION}"
tar -xzf "${DOWNLOAD_DIR}/zlib-${ZLIB_VERSION}.tar.gz" -C "${SRC_DIR}"
"${PREFIX}/bin/cmake" \
  -S "${SRC_DIR}/zlib-${ZLIB_VERSION}" \
  -B "${BUILD_DIR}/zlib-${ZLIB_VERSION}" \
  -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX="${PREFIX}"
"${PREFIX}/bin/cmake" --build "${BUILD_DIR}/zlib-${ZLIB_VERSION}" -j2
"${PREFIX}/bin/cmake" --install "${BUILD_DIR}/zlib-${ZLIB_VERSION}"

rm -rf "${SRC_DIR}/lz4-${LZ4_VERSION}" "${BUILD_DIR}/lz4-${LZ4_VERSION}"
tar -xzf "${DOWNLOAD_DIR}/lz4-${LZ4_VERSION}.tar.gz" -C "${SRC_DIR}"
"${PREFIX}/bin/cmake" \
  -S "${SRC_DIR}/lz4-${LZ4_VERSION}/build/cmake" \
  -B "${BUILD_DIR}/lz4-${LZ4_VERSION}" \
  -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX="${PREFIX}" \
  -DBUILD_SHARED_LIBS=ON \
  -DBUILD_STATIC_LIBS=ON
"${PREFIX}/bin/cmake" --build "${BUILD_DIR}/lz4-${LZ4_VERSION}" -j2
"${PREFIX}/bin/cmake" --install "${BUILD_DIR}/lz4-${LZ4_VERSION}"

rm -rf "${SRC_DIR}/c-blosc-${CBLOSC_VERSION}" "${BUILD_DIR}/c-blosc-${CBLOSC_VERSION}"
tar -xzf "${DOWNLOAD_DIR}/c-blosc-${CBLOSC_VERSION}.tar.gz" -C "${SRC_DIR}"
"${PREFIX}/bin/cmake" \
  -S "${SRC_DIR}/c-blosc-${CBLOSC_VERSION}" \
  -B "${BUILD_DIR}/c-blosc-${CBLOSC_VERSION}" \
  -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
  -DCMAKE_INSTALL_PREFIX="${PREFIX}" \
  -DCMAKE_PREFIX_PATH="${PREFIX}" \
  -DBUILD_TESTS=OFF \
  -DBUILD_FUZZERS=OFF \
  -DBUILD_BENCHMARKS=OFF \
  -DPREFER_EXTERNAL_ZSTD=ON \
  -DPREFER_EXTERNAL_ZLIB=ON \
  -DPREFER_EXTERNAL_LZ4=ON
"${PREFIX}/bin/cmake" --build "${BUILD_DIR}/c-blosc-${CBLOSC_VERSION}" -j2
"${PREFIX}/bin/cmake" --install "${BUILD_DIR}/c-blosc-${CBLOSC_VERSION}"

if command -v ldconfig >/dev/null 2>&1; then
  ldconfig
fi

echo "Installed latest native toolchain into ${PREFIX}"
"${PREFIX}/bin/cmake" --version | head -n 1
"${PREFIX}/bin/ninja" --version
grep -E '^#define ZSTD_VERSION_(MAJOR|MINOR|RELEASE)' "${PREFIX}/include/zstd.h"
grep -E '^#define ZLIB_VERSION' "${PREFIX}/include/zlib.h"
grep -E '^#define LZ4_VERSION_(MAJOR|MINOR|RELEASE)' "${PREFIX}/include/lz4.h"
grep -E '^#define BLOSC_VERSION_(MAJOR|MINOR|RELEASE)' "${PREFIX}/include/blosc.h"

# Native C ABI Interoperability

This document defines the optional shared-library API for external callers
that need to interact with the standalone native implementation without
running the CLI.

## Purpose

The product remains native C++. The C ABI is not a Python adapter, not pybind,
and not an embedded interpreter. It is a stable FFI boundary that any caller
capable of loading a C shared library can use.

The ABI exists because the original upstream package collaborated heavily with
other array and storage packages:

- documented writer entrypoints accept NumPy arrays, Dask arrays, Zarr groups,
  string paths, and storage options:
  `https://ome-zarr.readthedocs.io/en/stable/api/writer.html`
- documented scale entrypoints operate on NumPy or Dask arrays and use
  image-processing backends such as scikit-image and SciPy:
  `https://ome-zarr.readthedocs.io/en/stable/api/scale.html`
- the frozen source uses `zarr`, `dask.array`, `fsspec`, `numpy`, and
  image-processing libraries as package boundaries, while the native port must
  expose equivalent behavior without moving semantics back into Python objects.

## Contract

The ABI has two surface types:

- JSON calls for metadata-shaped functions, scalar conversions, and path/store
  signatures.
- Typed contiguous buffer calls for array-shaped data.

The JSON surface is reached through:

```c
OmeZarrNativeApiResult ome_zarr_native_api_call_json(
    const char* operation,
    const char* request_json);
```

The current JSON operations are:

- `api.available_operations`
- `conversions.int_to_rgba`
- `conversions.int_to_rgba_255`
- `conversions.rgba_to_int`
- `csv.parse_csv_value`
- `format.format_from_version`
- `io.local_io_signature`
- `scale.resize_image_shape`
- `scale.scaler_methods`

The current buffer operation is:

```c
OmeZarrNativeApiU8ArrayResult ome_zarr_native_api_rgb_to_5d_u8(
    const uint8_t* data,
    size_t ndim,
    const size_t* shape);
```

It accepts caller-owned C-contiguous `uint8` data and a caller-owned shape
array. It returns an owned output buffer and owned 5D shape. The caller must
free the result with `ome_zarr_native_api_free_u8_array_result`.

## NumPy And Array Package Interop

NumPy interoperability is tested through `ctypes` in
`tests/test_native_c_api_interop.py`. The test passes real NumPy array memory
to `ome_zarr_native_api_rgb_to_5d_u8`, copies the returned C-owned buffer into
NumPy, frees the C-owned result, and compares the shape and bytes against the
frozen upstream `ome_zarr.data.rgb_to_5d`.

This is the intended pattern for any package with host-contiguous array memory:

1. Make the input buffer contiguous and dtype-compatible.
2. Pass the raw pointer, dimension count, and shape to the C ABI.
3. Copy or consume the returned owned buffer.
4. Free the returned result with the matching API free function.

The ABI deliberately does not inspect NumPy objects. It only sees bytes and
shape metadata. That keeps the C++ side native and lets external packages
choose their own FFI layer.

## Zarr And Store Interop

The upstream package exposes Zarr stores and local paths as major public
boundaries. The C ABI exposes `io.local_io_signature` for local path/store
metadata signatures. The root test suite creates real Zarr stores through the
Python Zarr package as an external producer, calls the native C ABI against the
store path, and compares the result with the frozen upstream `parse_url`
location signature.

This verifies that the ABI can participate in workflows where an external
package creates or owns the store while the native library performs OME-Zarr
metadata interpretation.

## Non-Goals

- Do not add pybind, CPython headers, embedded interpreter calls, or `py::`
  objects to implement this ABI.
- Do not restore a repo-maintained Python compatibility package.
- Do not call the ABI complete for a new upstream surface until a root test
  compares that surface against the frozen upstream behavior.

## Verification

Run:

```bash
cmake -S . -B build-cpp-tests -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build-cpp-tests -j2
ctest --test-dir build-cpp-tests --output-on-failure
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q tests/test_native_c_api_interop.py
```

The pytest lane covers:

- C ABI metadata and operation discovery.
- Conversion parity for signed integer and RGBA edge cases.
- CSV scalar typing, including truthiness and non-finite floats.
- JSON dispatch parity for format metadata.
- NumPy `uint8` 2D, 3D, and zero-length array interop.
- Upstream-matching invalid-dimension error reporting.
- Zarr local store signature interop against a real Zarr-created store.
- Scale method and resize-shape parity against the frozen upstream.

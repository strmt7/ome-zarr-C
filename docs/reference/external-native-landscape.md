# External Native Landscape

This note records current evidence about non-Python or native-heavy OME-Zarr
projects so this repository does not duplicate prior work blindly.

## Current findings

### Image.sc discussion

- the Image.sc thread `OME-ZARR reading/writing with C++ libraries` focused on
  which native Zarr library to use from C++, not on porting `ome-zarr-py`
- participants repeatedly recommended `tensorstore` as the most mature C++
  Zarr option
- the same thread also mentioned `acquire-project/acquire-driver-zarr` as
  another C++ Zarr writer aimed at the Acquire project

### `acquire-project/acquire-zarr`

- C++ implementation with C and Python targets
- states in its README that it supports chunked, compressed, multiscale
  streaming to Zarr v3 with OME-NGFF metadata
- appears to be an independent streaming/writer-oriented project, not a direct
  port of `ome-zarr-py`
- GitHub code search for the exact string `ome-zarr-py` inside this repository
  returned no matches during this review

### `InsightSoftwareConsortium/ITKIOOMEZarrNGFF`

- C++ ITK external module for reading and writing images stored in Zarr-backed
  OME-NGFF
- Python usage is provided through ITK wrapping, but the core project is C++
- GitHub code search for the exact string `ome-zarr-py` inside this repository
  returned no matches during this review

### `zarrs/ome_zarr_metadata`

- Rust library focused on OME-Zarr metadata
- useful for metadata validation and serialization work, but not a direct port
  of the full `ome-zarr-py` read/write stack

## Negative finding

GitHub searches for the exact string `ome-zarr-py` in repository names,
descriptions, and C++ code did not surface an existing public C/C++ port of the
project itself. The evidence currently points to parallel native
implementations, not direct conversions of the Python codebase.

## Practical implication

If this repository continues the C++ path, it should assume that:

- native precedent exists
- direct `ome-zarr-py` port precedent does not currently appear to exist
- interoperability and feature comparison against native peers is still useful
- implementation work should be benchmarked and compared against both upstream
  `ome-zarr-py` and native alternatives where relevant

# OME-Zarr C++ Guide

Native C++ tools for inspecting, creating, transforming, viewing, and exporting
OME-Zarr data.

The native runtime is built from `cpp/native/` and exposed through:

- `ome_zarr_native_cli`: command-line workflows for local OME-Zarr stores
- `ome_zarr_native_probe`: structured JSON probes used by tests and automation
- `ome_zarr_native`: static C++ library target
- `ome_zarr_native_api`: optional C ABI shared library for FFI callers

Build first:

```bash
cmake -S . -B build-cpp -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build-cpp -j2
```

Check the command surface:

```bash
./build-cpp/ome_zarr_native_cli --help
./build-cpp/ome_zarr_native_cli --version
```

## Basic

### Write OME-Zarr Images

The native CLI can generate deterministic sample OME-Zarr images with labels.
Use this for smoke tests, demos, format checks, and local viewer fixtures.

```bash
./build-cpp/ome_zarr_native_cli create /tmp/coins.zarr
./build-cpp/ome_zarr_native_cli create --method coins --format 0.5 /tmp/coins-v05.zarr
./build-cpp/ome_zarr_native_cli create --method coins --format 0.4 /tmp/coins-v04.zarr
./build-cpp/ome_zarr_native_cli create --method astronaut --format 0.5 /tmp/astronaut.zarr
```

Available sample methods:

| Method | Output |
| --- | --- |
| `coins` | grayscale image pyramid plus label hierarchy |
| `astronaut` | RGB image pyramid plus circle label hierarchy |

Available OME-Zarr versions:

| `--format` | Storage layout |
| --- | --- |
| `0.5` | Zarr v3 metadata, `zarr.json`, default native output |
| `0.4` | Zarr v2-compatible legacy metadata |

Native C++ snippets below assume the native symbols are imported or qualified
in the translation unit.

Native C++ entrypoint:

```cpp
#include "create_runtime.hpp"

local_create_sample(
    "/tmp/coins.zarr",
    "coins",
    "coins",
    "0.5",
    CreateColorMode::native_random);
```

For reproducible label colors from automation, pass a seed:

```cpp
local_create_sample(
    "/tmp/astronaut.zarr",
    "astronaut",
    "circles",
    "0.5",
    CreateColorMode::native_random,
    std::uint64_t{0});
```

### Read OME-Zarr Images

Use `info` for metadata traversal. Add `--stats` only when you intentionally
want the command to read array chunks and report value ranges.

```bash
./build-cpp/ome_zarr_native_cli info /tmp/coins.zarr
./build-cpp/ome_zarr_native_cli info /tmp/coins.zarr --stats
```

Native C++ entrypoint:

```cpp
#include "local_runtime.hpp"

const auto lines = local_info_lines("/tmp/coins.zarr", false);
const auto lines_with_stats = local_info_lines("/tmp/coins.zarr", true);
```

Discover image roots below a directory:

```bash
./build-cpp/ome_zarr_native_cli finder /data/images --port 8012
```

Native C++ entrypoints:

```cpp
#include "local_runtime.hpp"

const auto images = local_walk_ome_zarr("/data/images");
const auto multiscales = local_find_multiscales("/data/images");
const auto finder = local_finder_csv("/data/images", 8012);
```

### View OME-Zarr Images

The native view command starts a local HTTP server rooted at the image parent
directory and opens a browser URL for validator-style viewing.

```bash
./build-cpp/ome_zarr_native_cli view /tmp/coins.zarr
./build-cpp/ome_zarr_native_cli view /tmp/coins.zarr --port 8020
./build-cpp/ome_zarr_native_cli view /tmp/coins.zarr --force
```

Use `--force` when you want to open the browser even if validation discovers no
image roots at the selected path.

Native C++ entrypoints:

```cpp
#include "local_runtime.hpp"

const auto prep = local_view_prepare("/tmp/coins.zarr", 8013, false);
local_view_run(prep, 8013);
```

### Write Labels

Sample creation writes labels automatically. Existing label properties can also
be updated from CSV:

```bash
./build-cpp/ome_zarr_native_cli csv_to_labels \
  /tmp/labels.csv \
  shape_id \
  "area#d,label_text#s,width#l,active#b" \
  /tmp/image.zarr \
  omero:shapeId
```

CSV column type suffixes:

| Suffix | Native value type |
| --- | --- |
| none | string |
| `#s` | string |
| `#d` | double |
| `#l` | signed integer |
| `#b` | boolean |

Native C++ entrypoints:

```cpp
#include "local_runtime.hpp"

const auto csv_result = local_csv_to_labels(
    "/tmp/labels.csv",
    "shape_id",
    "area#d,label_text#s,width#l,active#b",
    "/tmp/image.zarr",
    "omero:shapeId");

LocalDictToZarrEntry entry{
    true,
    "12345",
    nlohmann::ordered_json{{"area", 42.5}, {"active", true}},
};
const auto dict_result = local_dict_to_zarr(
    {entry},
    "/tmp/image.zarr",
    "omero:shapeId");
```

### Command-Line Tool

General shape:

```bash
./build-cpp/ome_zarr_native_cli <command> [options]
```

Supported commands:

| Command | Purpose |
| --- | --- |
| `info <path>` | Print discovered OME-Zarr nodes and metadata summary |
| `info <path> --stats` | Include min/max array statistics |
| `create [--method coins|astronaut] [--format 0.4|0.5] <path>` | Create sample data |
| `download <path> [--output DIR]` | Copy a local OME-Zarr store into an output directory, rewriting chunks and metadata as needed |
| `finder <path> [--port PORT]` | Build a BioFile Finder CSV for discovered images |
| `view <path> [--port PORT] [--force|-f]` | Serve a local directory and open a viewer URL |
| `scale <input_array> <output_directory> <axes> [options]` | Build a multiscale pyramid from a local input array |
| `csv_to_labels <csv_path> <csv_id> <csv_keys> <zarr_path> <zarr_id>` | Add CSV fields to label properties |

Verbosity flags are intentionally not part of the native CLI contract. Commands
print deterministic, testable output and return non-zero on invalid input.

## Advanced

### Sharding

Sharding groups multiple chunks into larger storage units. It is useful when
the file count itself becomes a bottleneck, especially on remote object stores
or filesystems with high per-file overhead.

Native read, info, and download paths understand Zarr metadata and chunk
layouts for the qualified v2/v3 local-store paths. The current native CLI does
not expose a stable writer flag for shard layout selection. For native writer
planning, keep chunk and shard decisions aligned with these rules:

- shard extents must be integer multiples of chunk extents
- small chunks improve selective reads but increase object count
- large shards reduce object count but can increase transfer size for small
  reads
- chunk shape should follow the access pattern, not only the full image shape

Current C++ planning helpers live in `cpp/native/writer.hpp`:

```cpp
#include "writer.hpp"

const auto pyramid = writer_pyramid_plan(
    {{1, 3, 128, 128, 128}, {1, 3, 64, 64, 64}},
    3,
    {"t", "c", "z", "y", "x"},
    {{1, 1, 32, 32, 32}, {1, 1, 32, 32, 32}});
```

### Customizing the Pyramid

Use `scale` to generate downsampled levels from an existing local array:

```bash
./build-cpp/ome_zarr_native_cli scale \
  /tmp/input.zarr \
  /tmp/output.zarr \
  zyx \
  --downscale 2 \
  --max_layer 4 \
  --method nearest \
  --copy-metadata
```

Options:

| Option | Meaning |
| --- | --- |
| `--downscale N` | Factor between levels; default is `2` |
| `--max_layer N` | Number of downsampled levels to create |
| `--method nearest` | Nearest-neighbor resampling, safest for labels |
| `--method resize` | Anti-aliased resize-style downsampling |
| `--method local_mean` | Local averaging for integer factor reductions |
| `--method zoom` | Zoom-style interpolation |
| `--copy-metadata` | Copy input attributes into the output group |

Native C++ entrypoint:

```cpp
#include "scale_runtime.hpp"

const auto result = local_scale_array(
    "/tmp/input.zarr",
    "/tmp/output.zarr",
    "zyx",
    true,
    "nearest",
    false,
    2,
    4);
```

Lower-level scale helpers:

```cpp
#include "scale.hpp"

const auto methods = scaler_methods();
const auto shape = scaler_resize_image_shape({1, 1, 1, 64, 64}, 2);
const auto levels = build_pyramid_plan(
    {1, 1, 1, 64, 64},
    {"t", "c", "z", "y", "x"},
    scale_levels_from_ints({"t", "c", "z", "y", "x"}, 4));
```

### Write HCS Plates

High-content screening data is organized as a plate root, row groups, column
groups, wells, fields, and image pyramids. The native reader and discovery
logic understand this hierarchy, and writer planning helpers validate plate
and well metadata rules.

Metadata planning helpers:

```cpp
#include "writer.hpp"

const auto plate_plan = writer_plate_metadata_plan("0.5");
const auto well_plan = writer_well_metadata_plan("0.5");

validate_plate_rows_columns({"A", "B"}, "rows");
validate_plate_rows_columns({"1", "2", "3"}, "columns");
validate_well_image(WellImageInput{
    false,
    true,
    "{'path': '0'}",
    true,
    true,
    "0",
    false,
    false,
    false,
});
```

Read and inspect an HCS tree:

```bash
./build-cpp/ome_zarr_native_cli info /tmp/plate.zarr
./build-cpp/ome_zarr_native_cli finder /tmp/plate.zarr
```

## Explanation

### Understanding OME-Zarr

OME-Zarr stores microscopy data as a Zarr hierarchy with additional metadata
for image semantics. The important building blocks are:

- multiscale datasets for fast rendering at different zoom levels
- named axes such as `t`, `c`, `z`, `y`, and `x`
- coordinate transformations, normally including at least a `scale`
- optional labels for segmentation-style integer masks
- optional HCS plate, well, and field hierarchy

The native runtime keeps these semantics in small C++ modules:

| Area | Native module |
| --- | --- |
| Axes | `cpp/native/axes.*` |
| Format metadata | `cpp/native/format.*` |
| Local store access | `cpp/native/io.*`, `cpp/native/local_runtime.*` |
| Reader hierarchy | `cpp/native/reader.*`, `cpp/native/reader_oracle.*` |
| Scaling | `cpp/native/scale.*`, `cpp/native/scale_runtime.*` |
| Writer metadata planning | `cpp/native/writer.*` |
| CSV label updates | `cpp/native/csv.*`, `cpp/native/local_runtime.*` |
| Synthetic data | `cpp/native/data.*`, `cpp/native/create_runtime.*` |

### Multiscale Pyramids

A multiscale pyramid stores level `0` at full resolution and later levels at
progressively lower resolution. Viewers can then request an appropriate level
instead of loading the entire full-resolution array.

Example level sequence:

```text
s0: 4096 x 4096
s1: 2048 x 2048
s2: 1024 x 1024
s3:  512 x  512
```

Method selection matters:

| Data | Recommended method |
| --- | --- |
| label masks and categorical values | `nearest` |
| continuous intensity images | `resize`, `local_mean`, or `zoom` depending on the desired smoothing |
| exact integer-factor averaging | `local_mean` |

For labels, avoid methods that invent intermediate values. Nearest-neighbor
preserves existing label IDs.

### Zarr Concepts

Zarr stores arrays in chunks. A group contains metadata and child arrays or
groups. A chunk is the smallest storage unit read for array data.

Native local-store conventions:

| Concept | v0.4 / Zarr v2 style | v0.5 / Zarr v3 style |
| --- | --- | --- |
| group metadata | `.zgroup`, `.zattrs` | `zarr.json` |
| array metadata | `.zarray`, `.zattrs` | `zarr.json` |
| chunk keys | v2 separators such as `.` or `/` | `c/...` with configured separator |
| codecs | v2 compressor metadata | v3 codec pipeline |

Performance guidance:

- choose chunk shapes that match expected reads
- keep labels and images in separate arrays under the same image hierarchy
- use pyramids for large images that will be viewed interactively
- use statistics commands carefully, because value statistics require reading
  array chunks

## API Reference

### Writer (`cpp/native/writer.hpp`)

Writer helpers plan metadata, validate inputs, and describe pyramid layout.

Important functions:

| Function | Use |
| --- | --- |
| `resolve_writer_format` | Resolve requested format against an existing group format |
| `writer_multiscales_metadata_plan` | Decide root and multiscales metadata placement |
| `writer_plate_metadata_plan` | Decide plate metadata placement |
| `writer_well_metadata_plan` | Decide well metadata placement |
| `writer_label_metadata_plan` | Decide image-label metadata placement |
| `writer_pyramid_plan` | Plan dataset components, dimension names, chunks, and key encoding |
| `writer_image_plan` | Plan image scale factors and method fallback behavior |
| `writer_labels_plan` | Plan label scale factors and label-safe method defaults |

### Reader (`cpp/native/reader.hpp`, `cpp/native/reader_oracle.hpp`)

Reader helpers identify which specifications apply to a node, compute child
traversal behavior, normalize multiscales metadata, and build HCS tile plans.

Important functions:

| Function | Use |
| --- | --- |
| `reader_matching_specs` | Determine applicable specs from metadata flags |
| `reader_node_add_plan` | Decide whether and how a child node is added |
| `reader_multiscales_summary` | Extract dataset paths and transformation presence |
| `reader_label_color_plan` | Normalize label color metadata |
| `reader_label_property_plan` | Normalize label property metadata |
| `reader_omero_plan` | Normalize channel visibility and display metadata |
| `reader_well_plan` | Derive well grid dimensions |
| `reader_plate_level_plans` | Derive stitched HCS tile paths |

Structured probe entrypoints:

```cpp
#include "reader_oracle.hpp"

const auto image = reader_probe_image_surface();
const auto plate = reader_probe_plate_surface();
const auto matches = reader_probe_matches("image");
```

### Input/output (`cpp/native/io.hpp`)

I/O helpers normalize paths, parse local-store metadata, and return structured
signatures for external callers.

Important functions:

| Function | Use |
| --- | --- |
| `io_basename` | Last path component for local or URL-like paths |
| `io_subpath` | Child path creation with local and HTTP-style rules |
| `io_repr` | Human-readable location representation |
| `io_parse_url_returns_none` | Open-mode existence behavior |
| `local_io_signature` | Structured metadata signature for a local store |

### Scale (`cpp/native/scale.hpp`, `cpp/native/scale_runtime.hpp`)

Scale helpers plan downsampled levels and execute local pyramid writing.

Important functions:

| Function | Use |
| --- | --- |
| `scaler_methods` | List available method names |
| `scaler_has_method` | Validate method names |
| `scaler_resize_image_shape` | Compute a resized shape |
| `scale_levels_from_ints` | Build default scale factors |
| `reorder_scale_levels` | Convert axis-keyed scale maps into axis order |
| `build_pyramid_plan` | Compute target shapes and relative factors |
| `local_scale_array` | Read a local array and write a multiscale output |

### Format (`cpp/native/format.hpp`)

Format helpers describe OME-Zarr version behavior and metadata validation.

Important functions:

| Function | Use |
| --- | --- |
| `format_class_name` | C++ class-style name for a version |
| `format_zarr_format` | Zarr major version for an OME-Zarr version |
| `format_chunk_key_encoding` | Chunk key separator policy |
| `detect_format` | Pick a format from metadata summary |
| `format_implementations` | Return known implementations newest-first |
| `validate_coordinate_transformations` | Validate scale/translation metadata groups |

### Command-Line (`cpp/native/cli.hpp`, `cpp/tools/native_cli.cpp`)

CLI planning helpers normalize parsed command arguments before runtime code
executes.

Important functions:

| Function | Use |
| --- | --- |
| `cli_create_plan` | Resolve sample method and default label name |
| `cli_download_plan` | Resolve output directory behavior |
| `cli_finder_plan` | Resolve finder port and paths |
| `cli_view_plan` | Resolve viewer port, warning behavior, and URL |
| `cli_scale_plan` | Resolve scaling command options |
| `cli_scale_factors` | Expand downscale/max-layer values into level factors |
| `cli_csv_to_labels_plan` | Normalize CSV label-update arguments |

### Utilities (`cpp/native/utils.hpp`, `cpp/native/local_runtime.hpp`)

Utility helpers discover images, prepare downloads, and support browser-facing
tools.

Important functions:

| Function | Use |
| --- | --- |
| `utils_path_split` | Split path text into normalized components |
| `utils_strip_common_prefix` | Remove shared path prefixes |
| `utils_single_multiscales_image` | Build a discovered-image record |
| `utils_finder_row` | Build a BioFile Finder CSV row |
| `utils_download_plan` | Plan output naming for downloads |
| `local_walk_ome_zarr` | Find image roots under a local directory |
| `local_download_copy` | Copy and rewrite a local OME-Zarr store |

### CSV (`cpp/native/csv.hpp`)

CSV helpers parse typed values and map CSV rows onto label properties.

Important functions:

| Function | Use |
| --- | --- |
| `parse_csv_value` | Parse string, double, integer, and boolean values |
| `parse_csv_key_specs` | Parse comma-separated column specs with type suffixes |
| `csv_props_by_id` | Build row properties keyed by an ID column |
| `csv_label_paths` | Decide which label groups are eligible for updates |
| `local_csv_to_labels` | Mutate label properties from a CSV file |
| `local_dict_to_zarr` | Mutate label properties from an in-memory map |

### Data (`cpp/native/data.hpp`, `cpp/native/create_runtime.hpp`)

Data helpers support synthetic examples, RGB layout conversion, and label mask
construction.

Important functions:

| Function | Use |
| --- | --- |
| `circle_points` | Compute points inside the default circular mask |
| `rgb_to_5d_shape` | Compute `(t, c, z, y, x)` output shape |
| `rgb_channel_order` | Determine channel order for RGB-like input |
| `coins_plan` | Synthetic coins image and label plan |
| `astronaut_plan` | Synthetic astronaut image and label plan |
| `create_zarr_plan` | Plan sample image metadata from version, shape, and chunks |
| `local_create_sample` | Write sample OME-Zarr data to disk |

### C ABI (`cpp/api/ome_zarr_native_api.h`)

The C ABI is the stable FFI boundary. It returns explicit success/error
records and requires callers to free returned buffers with the matching free
function.

Metadata and JSON calls:

```c
#include "ome_zarr_native_api.h"

OmeZarrNativeApiResult meta = ome_zarr_native_api_project_metadata();
ome_zarr_native_api_free_result(meta);

OmeZarrNativeApiResult methods = ome_zarr_native_api_call_json(
    "scale.scaler_methods",
    "{}");
ome_zarr_native_api_free_result(methods);
```

Direct scalar operations:

```c
OmeZarrNativeApiResult rgba = ome_zarr_native_api_int_to_rgba(16711935);
OmeZarrNativeApiResult value = ome_zarr_native_api_rgba_to_int(255, 0, 255, 255);
OmeZarrNativeApiResult parsed = ome_zarr_native_api_parse_csv_value("42.5", "d");

ome_zarr_native_api_free_result(rgba);
ome_zarr_native_api_free_result(value);
ome_zarr_native_api_free_result(parsed);
```

Buffer operation:

```c
uint8_t pixels[2 * 2 * 3] = {0};
size_t shape[3] = {2, 2, 3};

OmeZarrNativeApiU8ArrayResult out =
    ome_zarr_native_api_rgb_to_5d_u8(pixels, 3, shape);

ome_zarr_native_api_free_u8_array_result(out);
```

Supported JSON operation names:

```text
api.available_operations
conversions.int_to_rgba
conversions.int_to_rgba_255
conversions.rgba_to_int
csv.parse_csv_value
format.format_from_version
io.local_io_signature
scale.resize_image_shape
scale.scaler_methods
```

Supported buffer operation names:

```text
data.rgb_to_5d_u8
```

### Validation Commands

Run the native and parity checks before depending on a local build:

```bash
./build-cpp/ome_zarr_native_selftest
./build-cpp/ome_zarr_native_api_selftest
ctest --test-dir build-cpp --output-on-failure
```

Run repository-level parity tests:

```bash
.venv/bin/pytest -q
```

Run benchmark smoke checks:

```bash
./build-cpp/ome_zarr_native_bench_core --quick
./build-cpp/ome_zarr_native_bench_core --match reader. --quick
./build-cpp/ome_zarr_native_bench_core --match local.create --quick
```

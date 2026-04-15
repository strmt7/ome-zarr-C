# Iteration benchmark comparison: `public-api` / `csv.csv_to_zarr` vs native `local.csv_to_labels`

Python-visible harness timings come from the bounded `pyperf` suite.
Standalone native timings come from `ome_zarr_native_bench_core` and measure pure-native semantic cost.

| case | python us/op | cpp harness us/op | python/cpp speedup | native us/op | python/native speedup |
| --- | ---: | ---: | ---: | ---: | ---: |
| csv.csv_to_zarr | 5569.527 | 4003.473 | 1.391x | - | -x |
| local.csv_to_labels | - | - | -x | 462.587 | -x |

## Explicit paired comparisons

| python case | native case | python us/op | native us/op | python/native speedup |
| --- | --- | ---: | ---: | ---: |
| csv.csv_to_zarr | local.csv_to_labels | 5569.527 | 462.587 | 12.040x |

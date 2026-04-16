# Iteration benchmark comparison: `public-api` / `csv.csv_to_zarr` vs native `local.csv_to_labels`

Python timings come from the frozen upstream oracle via bounded `pyperf`.
Compatibility/oracle timings are Python package-path measurements, not standalone native C++ timings.
Standalone native timings come from `ome_zarr_native_bench_core` and measure pure-native semantic cost.

| case | python us/op | compat/oracle us/op | compat/oracle relative speed vs Python | native us/op | native C++ relative speed vs Python |
| --- | ---: | ---: | ---: | ---: | ---: |
| csv.csv_to_zarr | 5569.527 | 4003.473 | 1.391x | - | - |
| local.csv_to_labels | - | - | - | 462.587 | - |

## Explicit paired comparisons

| python case | native case | python us/op | native us/op | native C++ relative speed vs Python |
| --- | --- | ---: | ---: | ---: |
| csv.csv_to_zarr | local.csv_to_labels | 5569.527 | 462.587 | 12.040x |

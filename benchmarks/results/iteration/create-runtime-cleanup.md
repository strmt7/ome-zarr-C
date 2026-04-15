# Iteration benchmark comparison: `core` / `runtime.data.create_zarr_coins_v05` vs native `local.create_coins`

Python-visible harness timings come from the bounded `pyperf` suite.
Standalone native timings come from `ome_zarr_native_bench_core` and measure pure-native semantic cost.

| case | python us/op | cpp harness us/op | python/cpp speedup | native us/op | python/native speedup |
| --- | ---: | ---: | ---: | ---: | ---: |
| local.create_coins | - | - | -x | 6318.651 | -x |
| runtime.data.create_zarr_coins_v05 | 360144.822 | 13363.073 | 26.951x | - | -x |

## Explicit paired comparisons

| python case | native case | python us/op | native us/op | python/native speedup |
| --- | --- | ---: | ---: | ---: |
| runtime.data.create_zarr_coins_v05 | local.create_coins | 360144.822 | 6318.651 | 56.997x |

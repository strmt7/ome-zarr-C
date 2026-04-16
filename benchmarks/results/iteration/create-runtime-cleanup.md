# Iteration benchmark comparison: `core` / `runtime.data.create_zarr_coins_v05` vs native `local.create_coins`

Python timings come from the frozen upstream oracle via bounded `pyperf`.
Compatibility/oracle timings are Python package-path measurements, not standalone native C++ timings.
Standalone native timings come from `ome_zarr_native_bench_core` and measure pure-native semantic cost.

| case | python us/op | compat/oracle us/op | compat/oracle relative speed vs Python | native us/op | native C++ relative speed vs Python |
| --- | ---: | ---: | ---: | ---: | ---: |
| local.create_coins | - | - | - | 6318.651 | - |
| runtime.data.create_zarr_coins_v05 | 360144.822 | 13363.073 | 26.951x | - | - |

## Explicit paired comparisons

| python case | native case | python us/op | native us/op | native C++ relative speed vs Python |
| --- | --- | ---: | ---: | ---: |
| runtime.data.create_zarr_coins_v05 | local.create_coins | 360144.822 | 6318.651 | 56.997x |

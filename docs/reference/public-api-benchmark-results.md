# Public API Benchmark Results

Current bounded local public-API comparison between the frozen upstream Python oracle and standalone native C++ benchmark entrypoints.

Interpretation rule: native C++ speedup over Python is `python_time / native_cpp_time`. For example, if Python takes `10s` and C++ takes `1s`, C++ saves `9s`, uses `90%` less time, and is `10x` faster.

## Run

- Date: 2026-04-17
- Python timing source: frozen upstream oracle via bounded `pyperf`
- Native timing source: `./build-cpp/ome_zarr_native_bench_core --quick`
- Python command:

```bash
timeout 300s .venv/bin/python -m benchmarks.run \
  --suite public-api \
  --implementation python \
  --fast \
  --quiet \
  --processes 1 \
  --values 1 \
  --warmups 1 \
  --min-time 0.005 \
  --timeout 180 \
  --output /tmp/ome_zarr_c_public_api_python_deep_opt.pyperf.json
```

- Native command:

```bash
timeout 240s ./build-cpp/ome_zarr_native_bench_core \
  --quick \
  --json-output /tmp/ome_zarr_c_after_deep_opt.json
```

## Summary

- Paired public-API cases: 11
- Geometric-mean native C++ speedup over Python (`python_time / native_cpp_time`): 135.290x
- Every paired case in this run is faster in native C++ than in the frozen upstream Python oracle.

## Cases

| Case | Python time us/op | native C++ time us/op | time saved us/op | native C++ time reduction | native C++ speedup over Python (`python_time / native_cpp_time`) |
| --- | ---: | ---: | ---: | ---: | ---: |
| `axes.constructor_batch` | 68.739 | 0.135 | 68.603 | 99.8% | 507.327x |
| `conversions.int_to_rgba` | 4.102 | 0.020 | 4.082 | 99.5% | 205.078x |
| `conversions.rgba_to_int` | 1.002 | 0.021 | 0.981 | 98.0% | 48.854x |
| `csv.parse_csv_value` | 1.463 | 0.030 | 1.432 | 97.9% | 47.956x |
| `data.make_circle_batch` | 13.888 | 0.086 | 13.803 | 99.4% | 161.706x |
| `data.rgb_to_5d_batch` | 9.112 | 0.023 | 9.089 | 99.7% | 398.417x |
| `format.dispatch` | 3.143 | 0.052 | 3.091 | 98.3% | 60.033x |
| `format.matches` | 37.337 | 0.032 | 37.305 | 99.9% | 1164.407x |
| `format.v01_init_store` | 13.346 | 0.034 | 13.311 | 99.7% | 391.021x |
| `format.well_and_coord` | 7.414 | 0.232 | 7.181 | 96.9% | 31.943x |
| `utils.path_helpers` | 4.831 | 0.238 | 4.593 | 95.1% | 20.269x |

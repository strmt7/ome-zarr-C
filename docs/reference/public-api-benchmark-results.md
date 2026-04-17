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
  --output /tmp/ome_zarr_c_public_api_python_current.pyperf.json
```

- Native command:

```bash
timeout 120s ./build-cpp/ome_zarr_native_bench_core \
  --quick \
  --json-output /tmp/ome_zarr_c_public_api_native_after_opt.json
```

## Summary

- Paired public-API cases: 11
- Geometric-mean native C++ speedup over Python (`python_time / native_cpp_time`): 132.610x
- Every paired case in this run is faster in native C++ than in the frozen upstream Python oracle.

## Cases

| Case | Python time us/op | native C++ time us/op | time saved us/op | native C++ time reduction | native C++ speedup over Python (`python_time / native_cpp_time`) |
| --- | ---: | ---: | ---: | ---: | ---: |
| `axes.constructor_batch` | 69.803 | 0.129 | 69.674 | 99.8% | 541.991x |
| `conversions.int_to_rgba` | 4.110 | 0.020 | 4.090 | 99.5% | 205.520x |
| `conversions.rgba_to_int` | 1.002 | 0.020 | 0.982 | 98.0% | 50.085x |
| `csv.parse_csv_value` | 1.448 | 0.025 | 1.423 | 98.3% | 57.910x |
| `data.make_circle_batch` | 13.731 | 0.087 | 13.644 | 99.4% | 158.234x |
| `data.rgb_to_5d_batch` | 9.041 | 0.031 | 9.011 | 99.7% | 295.972x |
| `format.dispatch` | 3.116 | 0.058 | 3.058 | 98.1% | 53.474x |
| `format.matches` | 37.500 | 0.034 | 37.466 | 99.9% | 1116.436x |
| `format.v01_init_store` | 13.543 | 0.034 | 13.509 | 99.8% | 400.398x |
| `format.well_and_coord` | 7.467 | 0.234 | 7.233 | 96.9% | 31.928x |
| `utils.path_helpers` | 4.805 | 0.249 | 4.556 | 94.8% | 19.313x |

# Benchmark Summary

- Paired cases: 4
- Geometric-mean speedup (python / cpp): 0.887x
- Case classification: 1 cpp faster, 1 roughly equal, 2 cpp slower

## Group Geometric Means

- format: 0.887x

## Cases

| Case | Python median | C++ median | Speedup (py/cpp) | Status |
| --- | ---: | ---: | ---: | --- |
| dispatch | 48.62 us | 45.89 us | 1.059x | cpp faster |
| matches | 33.72 us | 38.51 us | 0.876x | cpp slower |
| v01_init_store | 186.66 us | 192.36 us | 0.970x | roughly equal |
| well_and_coord | 53.54 us | 77.78 us | 0.688x | cpp slower |

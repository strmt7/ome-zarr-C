# Benchmark Summary

- Paired cases: 4
- Geometric-mean compat/oracle relative speed vs Python: 0.887x
- Case classification: 1 above Python, 1 roughly equal, 2 below Python

## Group Geometric Means

- format: 0.887x

## Cases

| Case | Variant | Python median | converted median | converted relative speed vs Python | Status |
| --- | --- | ---: | ---: | ---: | --- |
| dispatch | compat/oracle | 48.62 us | 45.89 us | 1.059x | compat/oracle above Python |
| matches | compat/oracle | 33.72 us | 38.51 us | 0.876x | compat/oracle below Python |
| v01_init_store | compat/oracle | 186.66 us | 192.36 us | 0.970x | roughly equal |
| well_and_coord | compat/oracle | 53.54 us | 77.78 us | 0.688x | compat/oracle below Python |

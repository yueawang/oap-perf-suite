# oap-perf-suite
OAP Cluster Performance TestSuite

## Build
Use "sbt assembly" because it refers to org.reflections which may not exist in cluster env.

## Test
For cluster test:
OapBenchmark -c Config (oap/parquet, index/non-index, etc.) -s SuiteName (All, etc.) -t TestName (All, etc.) -r Repeat(3) 

For self test or local debug purpose:
OapBenchmark -bootstrapping

## TODO: Add Suite

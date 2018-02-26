# oap-perf-suite
OAP Cluster Performance TestSuite

## Build
sbt assembly because it refers to org.reflections which may not exist in cluster env.

## Test
OapBenchmark
-c Config (oap/parquet, index/non-index, etc.):
-s SuiteName (All, etc.):
-t TestName (All, etc.):
-r Repeat(3):
-bootstrapping: self test without cluster support.

## TODO: Add Suite

#!/bin/sh

OAP_PERF_SUITE_HOME=/home/oap/works/oap-perf-suite/
SPARK_HOME=/home/oap/spark/spark-2.1.0-bin-hadoop2.7
cd $SPARK_HOME

#sudo bin/spark-submit --master yarn --deploy-mode client --class org.apache.spark.sql.OapBenchmark $OAP_PERF_SUITE_HOME/target/scala-2.11/oap-perf-assembly-1.0.jar -r 3
#sudo bin/spark-submit --master yarn --deploy-mode client --class org.apache.spark.sql.OapBenchmark $OAP_PERF_SUITE_HOME/target/scala-2.11/oap-perf-assembly-1.0.jar -s OapStrategySuite -r 3
#sudo bin/spark-submit --master yarn --deploy-mode client --class org.apache.spark.sql.OapBenchmark $OAP_PERF_SUITE_HOME/target/scala-2.11/oap-perf-assembly-1.0.jar -t "Limit 10 from whole table" 
#sudo bin/spark-submit --master yarn --deploy-mode client --class org.apache.spark.sql.OapBenchmark $OAP_PERF_SUITE_HOME/target/scala-2.11/oap-perf-assembly-1.0.jar -c "spark.sql.oap.oindex.eis.enabled=false"

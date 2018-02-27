/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.suites

import org.apache.spark.sql._
import org.apache.spark.sql.internal.oap.OapConf

object BtreeIndexSuite extends OapTestSuite with OapPerfSuiteContext with ParquetVsOapConfigSet {
  override protected def getAppName: String = "BtreeIndexBenchmarkSuite"

  val table = "store_sales"

  val attr = "ss_ticket_number"

  def databaseName = {
    val conf = activeConf
    conf.getBenchmarkConf(BenchmarkConfig.FILE_FORMAT) match {
      case "parquet" => "parquet_tpcds_200"
      case "oap" => "oap_tpcds_200"
      case _ => "default"
    }
  }

  private def isDataBaseReady: Boolean = {
    if (spark.sqlContext.sql(s"show databases").collect().exists(_.getString(0) == databaseName)) {
      spark.sqlContext.sql(s"USE $databaseName")
      true
    } else {
      false
    }
  }

  private def isTableReady: Boolean = {
    if (spark.sqlContext.sql(s"show tables").collect().exists(_.getString(1) == table)) {
      val conf = activeConf
      if (conf.getBenchmarkConf(BenchmarkConfig.INDEX_ENABLE) == "true"){
        // Check if index exists.
        spark.sqlContext.sql(s"show oindex from $table").collect().exists(_.getString(3) == attr)
      } else {
        true
      }
    } else {
      false
    }
  }

  private def isDataReady(): Boolean = isDataBaseReady && isTableReady

  private def setRunningParams(): Boolean = {
    val conf = activeConf
    if (conf.getBenchmarkConf(BenchmarkConfig.INDEX_ENABLE) == "false"){
      spark.sqlContext.conf.setConf(OapConf.OAP_ENABLE_OINDEX, false)
    }

    true
  }

  override def prepare(): Boolean = {
    if (isDataReady()) {
      setRunningParams()
    } else {
      sys.error("ERROR: Data is not ready!")
      false
    }
  }

  /**
   * (name, sql sentence, TODO: profile, etc)
   */
  override def testSet = Seq(
    OapBenchmarkTest("$attr < ${Int.MaxValue}",
      s"SELECT * FROM $table WHERE $attr < ${Int.MaxValue}"),
    OapBenchmarkTest("$attr < 2000000",
      s"SELECT * FROM $table WHERE $attr < 2000000"),
    OapBenchmarkTest("$attr < 100000",
      s"SELECT * FROM $table WHERE $attr < 100000"),
    OapBenchmarkTest("$attr < 10000",
      s"SELECT * FROM $table WHERE $attr < 10000"),
    OapBenchmarkTest("$attr = 6000000",
      s"SELECT * FROM $table WHERE $attr = 6000000"),
    OapBenchmarkTest("$attr BETWEEN 100 AND 200",
      s"SELECT * FROM $table WHERE $attr BETWEEN 100 AND 200"),
    OapBenchmarkTest("$attr BETWEEN 100 AND 400",
      s"SELECT * FROM $table WHERE $attr BETWEEN 100 AND 400"),
    OapBenchmarkTest("$attr BETWEEN 100 AND 800",
      s"SELECT * FROM $table WHERE $attr BETWEEN 100 AND 800"),
    
    // Two columns query
    OapBenchmarkTest("$attr < 2000000 AND ss_customer_sk >= 120000",
      s"SELECT * FROM $table WHERE $attr < 2000000 AND ss_customer_sk >= 120000"),
    OapBenchmarkTest("$attr < 100000 AND ss_list_price < 100.0",
      s"SELECT * FROM $table WHERE $attr < 100000 AND ss_list_price < 100.0"),
    OapBenchmarkTest("$attr < 10000 AND ss_net_paid < 200.0",
      s"SELECT * FROM $table WHERE $attr < 10000 AND ss_net_paid > 100.0 AND ss_net_paid < 200.0"),
    OapBenchmarkTest("$attr < 10000 And ss_net_paid in [100.0, 110.0]",
      s"SELECT * FROM $table WHERE $attr < 10000 AND ss_net_paid BETWEEN 100.0 AND 110.0"),

    // Three columns query
    OapBenchmarkTest("$attr < 2000000 AND ss_customer_sk >= 120000 AND ss_list_price < 100.0",
      s"SELECT * FROM $table WHERE $attr < 2000000 AND ss_customer_sk >= 120000 AND ss_list_price < 100.0"),
    OapBenchmarkTest("$attr < 100000 AND ss_list_price < 100.0 AND ss_net_paid > 500.0",
      s"SELECT * FROM $table WHERE $attr < 100000 AND ss_list_price < 100.0 AND ss_net_paid > 500.0"),
    OapBenchmarkTest("$attr < 10000 AND AND ss_net_paid > 100.0 AND ss_net_paid < 110.0 AND ss_list_price < 100.0",
      s"SELECT * FROM $table WHERE $attr < 10000 AND ss_net_paid > 100.0 AND ss_net_paid < 110.0 AND ss_list_price < 100.0")
  )
}

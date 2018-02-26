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
package org.apache.spark.sql

import scala.collection.mutable

// TODO: use SQLConf style i.e. (value, defaultValue)
class BenchmarkConfig {
  // Benchmark config, include file format, index use or not, etc.
  private val benchmarkConf: mutable.HashMap[String, String] = mutable.HashMap.empty

  // SQLConf, send to Spark to change the sql query behavior.
  private val sqlConf: mutable.HashMap[String, String] = mutable.HashMap.empty

  def setBenchmarkConf(name: String, value: String): BenchmarkConfig = {
    benchmarkConf.put(name, value)
    this
  }

  def setSqlConf(name: String, value: String): BenchmarkConfig = {
    sqlConf.put(name, value)
    this
  }

  // Find a conf from either benchmark conf or sql conf.
  def getConf(name: String): String =
    benchmarkConf.get(name).getOrElse(sqlConf.get(name).getOrElse(s"$name Not Exist!!!"))

  def getBenchmarkConf(name: String): String = benchmarkConf.getOrElse(name, "false")

  def getSqlConf(name: String): String = sqlConf.getOrElse(name, "false")

  def allSqlOptions(): Map[String, String] = sqlConf.toMap[String, String]

  override def toString: String = {
    if (sqlConf.isEmpty) {
      val indexEnable = if (getBenchmarkConf(BenchmarkConfig.INDEX_ENABLE).toBoolean) {
        "W/ Index"
      } else {
        "W/O Index"
      }

      s"${getBenchmarkConf(BenchmarkConfig.FILE_FORMAT)} $indexEnable"
    } else {
      // oap !eis & statistics
      getBenchmarkConf(BenchmarkConfig.FILE_FORMAT) + " "
      sqlConf.toArray.map{ setting =>
        val flag = if (setting._2 == "true") {
          ""
        } else {
          "!"
        }
        flag + setting._1.split('.')(4)
      }.mkString(getBenchmarkConf(BenchmarkConfig.FILE_FORMAT) + " ", " & ", "")
    }
  }
}

object BenchmarkConfig {
  val INDEX_ENABLE = "oap.perf.config.index"
  val FILE_FORMAT  = "oap.perf.config.format"
}

abstract class BenchmarkConfigSelector {
  // TODO: choose conf
  def allConfigurations: Seq[BenchmarkConfig]

}

object BenchmarkConfigSelector {
  // TODO: build config accordingly.
  val wildcardConfiguration: mutable.HashMap[String, String] = mutable.HashMap.empty

  def build(options: Map[String, String]): Unit = {
    wildcardConfiguration ++= options
  }

  def isSelected(config: BenchmarkConfig): Boolean = {
    if (wildcardConfiguration.nonEmpty) {
      wildcardConfiguration.map{conf =>
        config.getConf(conf._1) == conf._2
      }.reduce(_ && _)
    } else {
      true
    }
  }
}

trait OapFileFormatParquetConfigSet extends BenchmarkConfigSelector{
  // TODO: choose conf
  def allConfigurations: Seq[BenchmarkConfig] = Seq(
    new BenchmarkConfig()
      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "parquet")
      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "true"),
    new BenchmarkConfig()
      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "parquet")
      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "false")
  )
}

trait OapFileFormatConfigSet extends BenchmarkConfigSelector{
  // TODO: choose conf
  def allConfigurations: Seq[BenchmarkConfig] = Seq(
    new BenchmarkConfig()
      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "oap")
      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "true"),
    new BenchmarkConfig()
      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "oap")
      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "false"),
    new BenchmarkConfig()
      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "parquet")
      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "true"),
    new BenchmarkConfig()
      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "parquet")
      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "false")
  )
}

trait OapSQLConfConfigSet extends BenchmarkConfigSelector{
  // TODO: choose conf
  def allConfigurations: Seq[BenchmarkConfig] = Seq(
    new BenchmarkConfig()
      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "oap")
      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "true")
      .setSqlConf("spark.sql.oap.oindex.eis.enabled", "false")
      .setSqlConf("spark.sql.oap.oindex.file.policy", "false")
      .setSqlConf("spark.sql.oap.oindex.statistics.policy", "false"),
    new BenchmarkConfig()
      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "oap")
      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "true")
      .setSqlConf("spark.sql.oap.oindex.eis.enabled", "true")
      .setSqlConf("spark.sql.oap.oindex.file.policy", "false")
      .setSqlConf("spark.sql.oap.oindex.statistics.policy", "false"),
    new BenchmarkConfig()
      .setBenchmarkConf(BenchmarkConfig.FILE_FORMAT, "oap")
      .setBenchmarkConf(BenchmarkConfig.INDEX_ENABLE, "true")
      .setSqlConf("spark.sql.oap.oindex.eis.enabled", "true")
      .setSqlConf("spark.sql.oap.oindex.file.policy", "true")
      .setSqlConf("spark.sql.oap.oindex.statistics.policy", "true")
  )
}
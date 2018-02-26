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

import com.databricks.spark.sql.perf.tpcds.Tables

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

import org.apache.spark.sql.functions._
import org.apache.spark.util.Utils

import scala.collection.mutable

object OapBenchmarkDataBuilder extends OapPerfContext {

  private val defaultProperties = Map(
    "oap.benchmark.compression.codec"     -> "gzip",
    "oap.benchmark.support.oap.version"   -> "0.3.0",
    "oap.benchmark.tpcds.tool.dir"        -> "/home/oap/tpcds-kit/tools",
    "oap.benchmark.hdfs.file.root.dir"    -> "/user/oap/oaptest/",
    "oap.benchmark.tpcds.data.scale"      -> "200",
    "oap.benchmark.tpcds.data.partition"  -> "80"
  )

  private val properties = new mutable.HashMap[String, String]

  override def beforeAll(): Unit = {
    super.beforeAll()
    try {
      Utils.getPropertiesFromFile("./oap-benchmark-default.conf").foreach{ case (k, v) =>
        properties(k) = v
      }
    } catch {
      case e: IllegalArgumentException =>{
        println(e.getMessage + ". Use default setting!")
        defaultProperties.foreach{ case (k, v) =>
          properties(k) = v
        }
      }
    }
  }

  def generateTables(): Unit = {
    val versionNum = properties.get("oap.benchmark.support.oap.version").get
    val codec = properties.get("oap.benchmark.compression.codec").get
    val scale = properties.get("oap.benchmark.tpcds.data.scale").get.toInt
    val partitions = properties.get("oap.benchmark.tpcds.data.partition").get.toInt
    val hdfsRootDir = properties.get("oap.benchmark.hdfs.file.root.dir").get
    val tpcdsToolPath = properties.get("oap.benchmark.tpcds.tool.dir").get

    spark.sqlContext.setConf("spark.sql.parquet.compression.codec", codec)
    val tables1 = new Tables(spark.sqlContext, tpcdsToolPath, scale)
    val oapLoc = s"${hdfsRootDir}/${versionNum}/tpcds/tpcds$scale/oap/"
    val parquetLoc = s"${hdfsRootDir}/${versionNum}/tpcds/tpcds$scale/parquet/"
    tables1.genData(
      oapLoc, "oap", true, false, true, false, false, "store_sales", partitions)
    // TODO: use copy method to make sure these two are exactly same.
    tables1.genData(
      parquetLoc, "parquet", true, false, true, false, false, "store_sales", partitions)
  }

  def generateDatabases() {
    val versionNum = properties.get("oap.benchmark.support.oap.version").get
    val dataScale = properties.get("oap.benchmark.tpcds.data.scale").get.toInt
    val hdfsRootDir = properties.get("oap.benchmark.hdfs.file.root.dir").get

    // "oap" or "parquet" or "both".
    // TODO: get from OapFileFormatConfigSet
    val dataFormats: Seq[String] = Seq("oap", "parquet")

    val conf = new Configuration()
    val hadoopFs = FileSystem.get(conf)
    val spark = SparkSession.builder.appName(s"OAP-Test-${versionNum}.0")
      .enableHiveSupport().getOrCreate()
    spark.sqlContext.setConf("spark.sql.parquet.compression.codec", "gzip")
    val sqlContext = spark.sqlContext
    val tablePath = s"${hdfsRootDir}/${versionNum}/tpcds/tpcds$dataScale/"
    spark.sql(s"create database if not exists oap_tpcds_$dataScale")
    spark.sql(s"create database if not exists parquet_tpcds_$dataScale")

    def genData(dataFormat: String) = {
      val dataLocation = s"${tablePath}${dataFormat}/"
      spark.sql(s"use ${dataFormat}_tpcds_${dataScale}")
      spark.sql("drop table if exists store_sales")
      spark.sql("drop table if exists store_sales_dup")

      /**
       * To compare performance between B-Tree and Bitmap index, we generate duplicate
       * tables of store_sales here. Besides, store_sales_dup table can be used in testing
       * OAP strategies.
       */
      val df = spark.read.format(dataFormat).load(dataLocation + "store_sales")
      val divRatio = df.select("ss_item_sk").orderBy(desc("ss_item_sk")).limit(1).
        collect()(0)(0).asInstanceOf[Int] / 1000
      val divideUdf = udf((s: Int) => s / divRatio)
      df.withColumn("ss_item_sk1", divideUdf(col("ss_item_sk"))).write.format(dataFormat)
        .mode(SaveMode.Overwrite).save(dataLocation + "store_sales1")
      hadoopFs.delete(new Path(dataLocation + "store_sales"), true)

      // Notice here delete source flag should firstly be set to false
      FileUtil.copy(hadoopFs, new Path(dataLocation + "store_sales1"),
        hadoopFs, new Path(dataLocation + "store_sales"), false, conf)
      FileUtil.copy(hadoopFs, new Path(dataLocation + "store_sales1"),
        hadoopFs, new Path(dataLocation + "store_sales_dup"), true, conf)
      sqlContext.createExternalTable("store_sales", dataLocation + "store_sales", dataFormat)
      sqlContext.createExternalTable("store_sales_dup", dataLocation + "store_sales_dup", dataFormat)
      println("File size of orignial table store_sales in oap format: " +
        TestUtil.calculateFileSize("store_sales", dataLocation, dataFormat)
      )
      println("Records of table store_sales: " +
        spark.read.format(dataFormat).load(dataLocation + "store_sales").count()
      )
    }

    dataFormats.foreach(genData)
  }

  def buildAllIndex() {
    val versionNum = properties.get("oap.benchmark.support.oap.version").get
    val dataScale = properties.get("oap.benchmark.tpcds.data.scale").get.toInt
    val hdfsRootDir = properties.get("oap.benchmark.hdfs.file.root.dir").get
    val dataFormats: Seq[String] = Seq("oap", "parquet")

    def buildBtreeIndex(tablePath: String, table: String, attr: String): Unit = {
      try {
        spark.sql(s"DROP OINDEX ${table}_${attr}_index ON $table")
      } catch {
        case _ => println("Index doesn't exist, so don't need to drop here!")
      } finally {
        TestUtil.time(
          spark.sql(
            s"CREATE OINDEX IF NOT EXISTS ${table}_${attr}_index ON $table ($attr) USING BTREE"
          ),
          s"Create B-Tree index on ${table}(${attr}) cost "
        )
        println(s"The size of B-Tree index on ${table}(${attr}) cost:" +
          TestUtil.calculateIndexSize(table, tablePath, attr))
      }
    }

    def buildBitmapIndex(tablePath: String, table: String, attr: String): Unit = {
      try {
        spark.sql(s"DROP OINDEX ${table}_${attr}_index ON $table")
      } catch {
        case _ => println("Index doesn't exist, so don't need to drop here!")
      } finally {
        TestUtil.time(
          spark.sql(
            s"CREATE OINDEX IF NOT EXISTS ${table}_${attr}_index ON $table ($attr) USING BITMAP"
          ),
          s"Create Bitmap index on ${table}(${attr}) cost"
        )
        println(s"The size of Bitmap index on ${table}(${attr}) cost:" +
          TestUtil.calculateIndexSize(table, tablePath, attr))
      }
    }

    dataFormats.foreach { dataFormat => {
        spark.sql(s"USE ${dataFormat}_tpcds_${dataScale}")
        val tablePath: String = s"$hdfsRootDir/${versionNum}/tpcds/tpcds${dataScale}/${dataFormat}/"
        buildBtreeIndex(tablePath, "store_sales", "ss_ticket_number")
        buildBitmapIndex(tablePath, "store_sales", "ss_item_sk1")
      }
    }
  }
}

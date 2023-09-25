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

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.catalyst.util.resourceToString

object PartitionSplitTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[4]")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.splitSourcePartition.enabled", "true")
      .config("spark.sql.splitSourcePartition.preferShuffle", "true")
      .config("spark.ui.port", "14040")
      .config("hive.metastore.uris", "thrift://cdh632-cm.kylin.com:9083")
      .enableHiveSupport()
      .appName("split-test")
      .getOrCreate()

    /* spark.sql(s"CREATE DATABASE SSB_P")
    spark.sql(s"CREATE TABLE SSB_P.LINEORDER STORED AS PARQUET AS select * from SSB.LINEORDER")
    spark.sql(s"CREATE TABLE SSB_P.PART STORED AS PARQUET AS select * from SSB.PART") */

    val ssbQueries = Seq("1.1")

    ssbQueries.foreach { name =>
      val queryString = resourceToString(
        s"split-test/$name.sql",
        classLoader = Thread.currentThread.getContextClassLoader)
      val frame = spark.sql(queryString)
      frame.show()
    }

    TimeUnit.MINUTES.sleep(5)
  }

}

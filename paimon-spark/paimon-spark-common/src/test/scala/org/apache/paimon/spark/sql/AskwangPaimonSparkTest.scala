/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.spark.sql

import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageSubmitted}

import scala.jdk.CollectionConverters._

/**
 * paimon spark test.
 */
class AskwangPaimonSparkTest extends PaimonSparkTestBase {

  test(s"Partition for partitioned table: tmp") {
    val hasPk = true
    val bucket = 4
    val prop = if (hasPk) {
      s"'primary-key'='a,b,dt,hh', 'bucket' = '$bucket' "
    } else if (bucket != -1) {
      s"'bucket-key'='a,b', 'bucket' = '$bucket' "
    } else {
      "'write-only'='true'"
    }

    spark.sql(s"""
                 |CREATE TABLE T (a VARCHAR(10), b CHAR(10),c BIGINT,dt LONG,hh VARCHAR(4))
                 |PARTITIONED BY (dt, hh)
                 |TBLPROPERTIES ($prop)
                 |""".stripMargin)

    spark.sql("INSERT INTO T VALUES('a','b',1,20230816,'1133')")
    spark.sql("INSERT INTO T VALUES('a','b',2,20230817,'1134')")
    spark.sql("INSERT INTO T VALUES('a','b',1,20230816,'1132')")
    spark.sql("INSERT INTO T VALUES('a','b',1,20230816,'1134')")
    spark.sql("INSERT INTO T VALUES('a','b',2,20230817,'1132')")
    spark.sql("INSERT INTO T VALUES('a','b',2,20230817,'1133')")
    spark.sql("INSERT INTO T VALUES('a','b',2,20240101,'00')")
    spark.sql("INSERT INTO T VALUES('a','b',2,20240102,'00')")

    spark.sql("show partitions T ").show(false)

    println("=====")
    spark.sql("select * from `T$buckets`").show(false)
    println("xxx")
  }

  test("Paimon Procedure: test aware-bucket compaction read parallelism") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, value STRING)
                 |TBLPROPERTIES ('primary-key'='id', 'bucket'='3', 'write-only'='true')
                 |""".stripMargin)

    val table = loadTable("T")
    for (i <- 1 to 10) {
      sql(s"INSERT INTO T VALUES ($i, '$i')")
    }
    assertResult(10)(table.snapshotManager().snapshotCount())

    val buckets = table.newSnapshotReader().bucketEntries().asScala.map(_.bucket()).distinct.size
    assertResult(3)(buckets)

    val taskBuffer = scala.collection.mutable.ListBuffer.empty[Int]
    val listener = new SparkListener {
      override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
        taskBuffer += stageSubmitted.stageInfo.numTasks
      }
    }

    try {
      spark.sparkContext.addSparkListener(listener)

      // spark.default.parallelism cannot be change in spark session
      // sparkParallelism is 2, bucket is 3, use 2 as the read parallelism
      spark.conf.set("spark.sql.shuffle.partitions", 2)
      spark.sql("CALL sys.compact(table => 'T')")

      // sparkParallelism is 5, bucket is 3, use 3 as the read parallelism
      spark.conf.set("spark.sql.shuffle.partitions", 5)
      spark.sql("CALL sys.compact(table => 'T')")

      assertResult(Seq(2, 3))(taskBuffer)
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
  }

}

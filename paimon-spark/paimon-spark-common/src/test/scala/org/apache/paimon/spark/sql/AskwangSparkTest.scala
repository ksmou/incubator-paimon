package org.apache.paimon.spark.sql

import org.apache.paimon.spark.PaimonHiveTestBase
import org.apache.spark.sql.internal.SQLConf

/**
 * only spark test， not related to paimon.
 */
class AskwangSparkTest extends PaimonHiveTestBase {

  test("create table/create table like/create table as select") {
    withTable("tb") {
      withSQLConf(SQLConf.PLAN_CHANGE_LOG_LEVEL.key -> "INFO",
        SQLConf.DATETIME_JAVA8API_ENABLED.key -> "true") {
        sql("CREATE TABLE `tb1`(i INT, dt TIMESTAMP) USING parquet")
        sql("show create table tb1").show(false)

        // sql("CREATE TABLE `tb2` like `tb1`")
      }
    }
  }
}

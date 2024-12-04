package org.apache.paimon.spark.orphan

import org.apache.paimon.catalog.{Catalog, Identifier}
import org.apache.paimon.fs.Path
import org.apache.paimon.spark.orphan.SparkOrphanFilesClean.conf
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.utils.SerializableConsumer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.SQLConfHelper

import scala.collection.JavaConverters._

/**
 * RemoveOrphanFilesProcedure 设置 dry_run=true，返回的就是 orphan files 数量。
 * 这种case其实可以简化，只需要对bucket中的文件数和所有snapshot的文件数进行对比即可。
 *
 * orphans files 不只是 data file，还有 manifest files。
 */
object SparkOrphanFilesCollect extends SQLConfHelper{

  def collectOrphanFiles(
                                  catalog: Catalog,
                                  databaseName: String,
                                  tableName: String,
                                  olderThanMillis: Long,
                                  fileCleaner: SerializableConsumer[Path],
                                  parallelismOpt: Integer): Long = {

    val spark = SparkSession.active
    val parallelism = if (parallelismOpt == null) {
      Math.max(spark.sparkContext.defaultParallelism, conf.numShufflePartitions)
    } else {
      parallelismOpt.intValue()
    }

    val identifier = new Identifier(databaseName, tableName)
    val table = catalog.getTable(identifier)
    assert(
      table.isInstanceOf[FileStoreTable],
      s"Only FileStoreTable supports collect-orphan-files action. The table type is '${table.getClass.getName}'.")
    val fileStoreTable = table.asInstanceOf[FileStoreTable]

    0L
  }

}

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

package org.apache.paimon.spark;

import org.apache.paimon.fs.Path;
import org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class AskwangPaimonSparkTest extends PaimonSparkTestBase {
    @Test
    public void testXXX(@TempDir java.nio.file.Path tempDir) {
        // firstly, we use hive metastore to creata table, and check the result.
        Path warehousePath = new Path("file:" + tempDir.toString());
        SparkSession spark =
                SparkSession.builder()
                        .config("spark.sql.warehouse.dir", warehousePath.toString())
                        // with hive metastore
                        .config("spark.sql.catalogImplementation", "hive")
                        .config(
                                "spark.sql.catalog.spark_catalog",
                                SparkGenericCatalog.class.getName())
                        .config(
                                "spark.sql.extensions",
                                PaimonSparkSessionExtensions.class.getName())
                        .master("local[2]")
                        .getOrCreate();

        spark.sql("CREATE DATABASE my_db");
        spark.sql("USE my_db");
        spark.sql(
                "create table T (id int, name string) "
                        + "using paimon "
                        + "TBLPROPERTIES('bucket'='1', 'primary-key'='id', 'metastore.tag-to-partition' = 'dt')");

        spark.sql("insert into T values (1,'a'),(2,'b'),(3,'c')");

        spark.sql("select * from T").show(false);

        spark.sql("CALL sys.create_tag(table => 'T', tag => '2024-09-01')");

        spark.sql("show partitions T").show();

        // spark.sql("select * from T where dt='2024-09-01").show();
    }
}

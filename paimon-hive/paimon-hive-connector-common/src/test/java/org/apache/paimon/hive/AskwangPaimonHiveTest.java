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

package org.apache.paimon.hive;

import org.apache.paimon.hive.annotation.Minio;
import org.apache.paimon.hive.runner.PaimonEmbeddedHiveRunner;
import org.apache.paimon.s3.MinioTestContainer;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.model.Statement;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/** IT cases for using Paimon {@link HiveCatalog} together with Paimon Hive connector. */
@RunWith(PaimonEmbeddedHiveRunner.class)
public abstract class AskwangPaimonHiveTest {

    @Rule public TemporaryFolder folder = new TemporaryFolder();

    protected String path;
    protected TableEnvironment tEnv;
    protected TableEnvironment sEnv;
    private boolean locationInProperties;

    @HiveSQL(files = {})
    protected static HiveShell hiveShell;

    @Minio private static MinioTestContainer minioTestContainer;

    private void before(boolean locationInProperties) throws Exception {
        this.locationInProperties = locationInProperties;
        if (locationInProperties) {
            path = minioTestContainer.getS3UriForDefaultBucket() + "/" + UUID.randomUUID();
        } else {
            path = folder.newFolder().toURI().toString();
        }
        registerHiveCatalog("my_hive", new HashMap<>());

        tEnv.executeSql("USE CATALOG my_hive").await();
        tEnv.executeSql("DROP DATABASE IF EXISTS test_db CASCADE");
        tEnv.executeSql("CREATE DATABASE test_db").await();
        tEnv.executeSql("USE test_db").await();

        sEnv.executeSql("USE CATALOG my_hive").await();
        sEnv.executeSql("USE test_db").await();

        hiveShell.execute("USE test_db");
        hiveShell.execute("CREATE TABLE hive_table ( a INT, b STRING )");
        hiveShell.execute("INSERT INTO hive_table VALUES (100, 'Hive'), (200, 'Table')");
    }

    private void registerHiveCatalog(String catalogName, Map<String, String> catalogProperties)
            throws Exception {
        catalogProperties.put("type", "paimon");
        catalogProperties.put("metastore", "hive");
        catalogProperties.put("uri", "");
        catalogProperties.put("lock.enabled", "true");
        catalogProperties.put("location-in-properties", String.valueOf(locationInProperties));
        catalogProperties.put("warehouse", path);
        if (locationInProperties) {
            catalogProperties.putAll(minioTestContainer.getS3ConfigOptions());
        }

        tEnv = TableEnvironmentImpl.create(EnvironmentSettings.newInstance().inBatchMode().build());
        sEnv =
                TableEnvironmentImpl.create(
                        EnvironmentSettings.newInstance().inStreamingMode().build());
        sEnv.getConfig()
                .getConfiguration()
                .set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(1));
        sEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);

        tEnv.executeSql(
                        String.join(
                                "\n",
                                "CREATE CATALOG " + catalogName + " WITH (",
                                catalogProperties.entrySet().stream()
                                        .map(
                                                e ->
                                                        String.format(
                                                                "'%s' = '%s'",
                                                                e.getKey(), e.getValue()))
                                        .collect(Collectors.joining(",\n")),
                                ")"))
                .await();

        sEnv.registerCatalog(catalogName, tEnv.getCatalog(catalogName).get());
    }

    private void after() {
        hiveShell.execute("DROP DATABASE IF EXISTS test_db CASCADE");
        hiveShell.execute("DROP DATABASE IF EXISTS test_db2 CASCADE");
    }

    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.RUNTIME)
    private @interface LocationInProperties {}

    @Rule
    public TestRule environmentRule =
            (base, description) ->
                    new Statement() {
                        @Override
                        public void evaluate() throws Throwable {
                            try {
                                before(
                                        description.getAnnotation(LocationInProperties.class)
                                                != null);
                                base.evaluate();
                            } finally {
                                after();
                            }
                        }
                    };

    @Test
    public void testAddPartitionsForTag222() throws Exception {
        tEnv.executeSql(
                String.join(
                        "\n",
                        "CREATE TABLE t (",
                        "    k INT,",
                        "    v BIGINT,",
                        "    PRIMARY KEY (k) NOT ENFORCED",
                        ") WITH (",
                        "    'bucket' = '2',",
                        "    'metastore.tag-to-partition' = 'dt'",
                        ")"));
        tEnv.executeSql("INSERT INTO t VALUES (1, 10), (2, 20)").await();
        tEnv.executeSql("CALL sys.create_tag('test_db.t', '2023-10-16', 1)");

        hiveShell.executeQuery("SHOW PARTITIONS t");
        System.out.println("end");
    }
}

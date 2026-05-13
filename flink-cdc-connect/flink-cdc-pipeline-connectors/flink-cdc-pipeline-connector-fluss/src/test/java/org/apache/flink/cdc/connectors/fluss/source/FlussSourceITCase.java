/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.fluss.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.fluss.source.deserializer.FlussRecordDeserializer;
import org.apache.flink.cdc.connectors.fluss.source.subscriber.FlussTableSubscriber;
import org.apache.flink.cdc.connectors.fluss.source.subscriber.PatternSubscriber;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.CloseableIterator;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.fluss.config.ConfigOptions.BOOTSTRAP_SERVERS;
import static org.apache.fluss.server.testutils.FlussClusterExtension.BUILTIN_DATABASE;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for {@link FlussSource} as a CDC pipeline source. */
public class FlussSourceITCase {

    private static final Logger LOG = LoggerFactory.getLogger(FlussSourceITCase.class);
    private static final int MAX_PARALLELISM = 4;
    private static final String DATABASE_NAME = "test_source_db";
    private static final Duration COLLECT_TIMEOUT = Duration.ofSeconds(60);

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(MAX_PARALLELISM)
                            .build());

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setClusterConf(initConfig())
                    .setNumOfTabletServers(3)
                    .build();

    protected TableEnvironment tBatchEnv;

    @BeforeEach
    void before() throws Exception {
        waitForFlussClusterReady();
        String bootstrapServers = FLUSS_CLUSTER_EXTENSION.getBootstrapServers();
        tBatchEnv =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        tBatchEnv.executeSql(
                String.format(
                        "CREATE CATALOG test_catalog WITH ('type' = 'fluss', '%s' = '%s')",
                        BOOTSTRAP_SERVERS.key(), bootstrapServers));
        tBatchEnv.executeSql("USE CATALOG test_catalog");
        tBatchEnv
                .getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        tBatchEnv.executeSql("CREATE DATABASE " + DATABASE_NAME);
        tBatchEnv.useDatabase(DATABASE_NAME);
    }

    @AfterEach
    void after() {
        tBatchEnv.useDatabase(BUILTIN_DATABASE);
        tBatchEnv.executeSql(String.format("DROP DATABASE %s CASCADE", DATABASE_NAME));
    }

    @Test
    void testNonPartitionedPkTable() throws Exception {
        String tableName = "pk_table";
        tBatchEnv
                .executeSql(
                        String.format(
                                "CREATE TABLE %s (id INT, name STRING, PRIMARY KEY (id) NOT ENFORCED)",
                                tableName))
                .await();

        tBatchEnv
                .executeSql(
                        String.format(
                                "INSERT INTO %s VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
                                tableName))
                .await();

        FlussSource<Event> source = createFlussSource(DATABASE_NAME, tableName, "earliest");
        List<Event> allEvents = collectAllEvents(source, 4, COLLECT_TIMEOUT);
        assertCreateTableEvents(allEvents, tableName);
        List<DataChangeEvent> events = filterDataChangeEvents(allEvents);

        List<String> actual = convertToStringList(events, DataTypes.INT(), DataTypes.STRING());
        assertThat(actual)
                .containsExactlyInAnyOrder("+I[1, Alice]", "+I[2, Bob]", "+I[3, Charlie]");
    }

    @Test
    void testNonPartitionedLogTable() throws Exception {
        String tableName = "log_table";
        tBatchEnv
                .executeSql(String.format("CREATE TABLE %s (id INT, name STRING)", tableName))
                .await();

        tBatchEnv
                .executeSql(
                        String.format(
                                "INSERT INTO %s VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie'), (4, 'David'), (5, 'Eve')",
                                tableName))
                .await();

        FlussSource<Event> source = createFlussSource(DATABASE_NAME, tableName, "earliest");
        List<Event> allEvents = collectAllEvents(source, 6, COLLECT_TIMEOUT);
        assertCreateTableEvents(allEvents, tableName);
        List<DataChangeEvent> events = filterDataChangeEvents(allEvents);

        List<String> actual = convertToStringList(events, DataTypes.INT(), DataTypes.STRING());
        assertThat(actual)
                .containsExactlyInAnyOrder(
                        "+I[1, Alice]",
                        "+I[2, Bob]",
                        "+I[3, Charlie]",
                        "+I[4, David]",
                        "+I[5, Eve]");
    }

    @Test
    void testPartitionedPkTable() throws Exception {
        String tableName = "part_pk_table";
        tBatchEnv
                .executeSql(
                        String.format(
                                "CREATE TABLE %s ("
                                        + "id INT, ds STRING, name STRING, "
                                        + "PRIMARY KEY (id, ds) NOT ENFORCED"
                                        + ") PARTITIONED BY (ds)",
                                tableName))
                .await();

        tBatchEnv
                .executeSql(
                        String.format(
                                "INSERT INTO %s VALUES "
                                        + "(1, '20240101', 'Alice'), (2, '20240101', 'Bob'), "
                                        + "(3, '20240102', 'Charlie'), (4, '20240102', 'David')",
                                tableName))
                .await();

        FlussSource<Event> source = createFlussSource(DATABASE_NAME, tableName, "earliest");
        List<Event> allEvents = collectAllEvents(source, 5, COLLECT_TIMEOUT, 1);
        assertCreateTableEvents(allEvents, tableName);
        List<DataChangeEvent> events = filterDataChangeEvents(allEvents);

        List<String> actual =
                convertToStringList(
                        events, DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING());
        assertThat(actual)
                .containsExactlyInAnyOrder(
                        "+I[1, 20240101, Alice]",
                        "+I[2, 20240101, Bob]",
                        "+I[3, 20240102, Charlie]",
                        "+I[4, 20240102, David]");
    }

    @Test
    void testMixedTables() throws Exception {
        String pkTable = "mixed_pk";
        String logTable = "mixed_log";
        String partTable = "mixed_part";

        tBatchEnv
                .executeSql(
                        String.format(
                                "CREATE TABLE %s (id INT, val STRING, PRIMARY KEY (id) NOT ENFORCED)",
                                pkTable))
                .await();
        tBatchEnv
                .executeSql(String.format("CREATE TABLE %s (id INT, val STRING)", logTable))
                .await();
        tBatchEnv
                .executeSql(
                        String.format(
                                "CREATE TABLE %s ("
                                        + "id INT, ds STRING, val STRING, "
                                        + "PRIMARY KEY (id, ds) NOT ENFORCED"
                                        + ") PARTITIONED BY (ds)",
                                partTable))
                .await();

        tBatchEnv
                .executeSql(String.format("INSERT INTO %s VALUES (1, 'pk1'), (2, 'pk2')", pkTable))
                .await();
        tBatchEnv
                .executeSql(
                        String.format("INSERT INTO %s VALUES (1, 'log1'), (2, 'log2')", logTable))
                .await();
        tBatchEnv
                .executeSql(
                        String.format(
                                "INSERT INTO %s VALUES (1, '20240101', 'part1'), (2, '20240102', 'part2')",
                                partTable))
                .await();

        // Use wildcard pattern to read all tables
        FlussSource<Event> source = createFlussSource(DATABASE_NAME, "mixed_*", "earliest");
        List<Event> allEvents = collectAllEvents(source, 9, COLLECT_TIMEOUT, 1);
        assertCreateTableEvents(allEvents, pkTable, logTable, partTable);
        List<DataChangeEvent> events = filterDataChangeEvents(allEvents);

        // Group events by table and verify each table's records
        Map<String, List<DataChangeEvent>> eventsByTable =
                events.stream().collect(Collectors.groupingBy(e -> e.tableId().getTableName()));

        List<String> pkEvents =
                convertToStringList(
                        eventsByTable.get(pkTable), DataTypes.INT(), DataTypes.STRING());
        assertThat(pkEvents).containsExactlyInAnyOrder("+I[1, pk1]", "+I[2, pk2]");

        List<String> logEvents =
                convertToStringList(
                        eventsByTable.get(logTable), DataTypes.INT(), DataTypes.STRING());
        assertThat(logEvents).containsExactlyInAnyOrder("+I[1, log1]", "+I[2, log2]");

        List<String> partEvents =
                convertToStringList(
                        eventsByTable.get(partTable),
                        DataTypes.INT(),
                        DataTypes.STRING(),
                        DataTypes.STRING());
        assertThat(partEvents)
                .containsExactlyInAnyOrder("+I[1, 20240101, part1]", "+I[2, 20240102, part2]");
    }

    @Test
    void testEarliestStartupMode() throws Exception {
        String tableName = "earliest_test";
        tBatchEnv
                .executeSql(
                        String.format(
                                "CREATE TABLE %s (id INT, name STRING, PRIMARY KEY (id) NOT ENFORCED)",
                                tableName))
                .await();
        tBatchEnv
                .executeSql(
                        String.format(
                                "INSERT INTO %s VALUES (1, 'A'), (2, 'B'), (3, 'C')", tableName))
                .await();

        FlussSource<Event> source = createFlussSource(DATABASE_NAME, tableName, "earliest");
        List<Event> allEvents = collectAllEvents(source, 4, COLLECT_TIMEOUT);
        assertCreateTableEvents(allEvents, tableName);
        List<DataChangeEvent> events = filterDataChangeEvents(allEvents);

        List<String> actual = convertToStringList(events, DataTypes.INT(), DataTypes.STRING());
        assertThat(actual).containsExactlyInAnyOrder("+I[1, A]", "+I[2, B]", "+I[3, C]");
    }

    @Test
    void testLatestStartupMode() throws Exception {
        // todo: 这个测试实现的不是很好，后面再实现一次。
        String tableName = "latest_test";
        tBatchEnv
                .executeSql(
                        String.format(
                                "CREATE TABLE %s (id INT, name STRING, PRIMARY KEY (id) NOT ENFORCED)",
                                tableName))
                .await();

        // Write initial data BEFORE starting the source
        tBatchEnv
                .executeSql(
                        String.format("INSERT INTO %s VALUES (1, 'Old1'), (2, 'Old2')", tableName))
                .await();

        // Start source in "latest" mode in background
        FlussSource<Event> source = createFlussSource(DATABASE_NAME, tableName, "latest");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        List<Event> collectedEvents = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(3);

        CloseableIterator<Event> iter =
                env.fromSource(
                                source,
                                WatermarkStrategy.noWatermarks(),
                                "FlussSource",
                                new EventTypeInfo())
                        .executeAndCollect("LatestModeTest");

        Thread collector =
                new Thread(
                        () -> {
                            try {
                                while (iter.hasNext()) {
                                    Event event = iter.next();
                                    collectedEvents.add(event);
                                    latch.countDown();
                                }
                            } catch (Exception ignored) {
                            }
                        },
                        "latest-collector");
        collector.setDaemon(true);
        collector.start();

        // Wait for source to be initialized
        Thread.sleep(5000);

        // Write new data AFTER source started
        tBatchEnv
                .executeSql(
                        String.format("INSERT INTO %s VALUES (3, 'New1'), (4, 'New2')", tableName))
                .await();

        try {
            latch.await(COLLECT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } finally {
            iter.close();
            collector.join(5000);
        }

        // Should only receive the NEW data (written after source started)
        // todo: 现在分别filter createTableEvent和dataEvents不是很能体现出顺序，后续需要优化
        assertCreateTableEvents(collectedEvents, tableName);
        List<DataChangeEvent> dataEvents = filterDataChangeEvents(collectedEvents);
        List<String> actual = convertToStringList(dataEvents, DataTypes.INT(), DataTypes.STRING());
        assertThat(actual).containsExactlyInAnyOrder("+I[3, New1]", "+I[4, New2]");
    }

    @Test
    void testFullStartupMode() throws Exception {
        String tableName = "full_test";
        tBatchEnv
                .executeSql(
                        String.format(
                                "CREATE TABLE %s (id INT, name STRING, PRIMARY KEY (id) NOT ENFORCED)",
                                tableName))
                .await();

        // Write initial data (will be captured by KV snapshot)
        tBatchEnv
                .executeSql(
                        String.format(
                                "INSERT INTO %s VALUES (1, 'Snap1'), (2, 'Snap2'), (3, 'Snap3')",
                                tableName))
                .await();

        // Wait for KV snapshot to be taken (configured interval is 1s)
        Thread.sleep(3000);

        // Write more data (will be in log after snapshot)
        tBatchEnv
                .executeSql(
                        String.format("INSERT INTO %s VALUES (4, 'Log1'), (5, 'Log2')", tableName))
                .await();

        // Start source in "full" mode - should read snapshot + log
        FlussSource<Event> source = createFlussSource(DATABASE_NAME, tableName, "full");
        List<Event> allEvents = collectAllEvents(source, 6, COLLECT_TIMEOUT);
        assertCreateTableEvents(allEvents, tableName);
        List<DataChangeEvent> events = filterDataChangeEvents(allEvents);

        // All 5 records should be present (3 from snapshot + 2 from log)
        List<String> actual = convertToStringList(events, DataTypes.INT(), DataTypes.STRING());
        assertThat(actual)
                .containsExactlyInAnyOrder(
                        "+I[1, Snap1]",
                        "+I[2, Snap2]",
                        "+I[3, Snap3]",
                        "+I[4, Log1]",
                        "+I[5, Log2]");
    }

    @Test
    void testTimestampStartupMode() throws Exception {
        String tableName = "timestamp_test";
        tBatchEnv
                .executeSql(
                        String.format(
                                "CREATE TABLE %s (id INT, name STRING, PRIMARY KEY (id) NOT ENFORCED)",
                                tableName))
                .await();

        // Write phase 1 data
        tBatchEnv
                .executeSql(
                        String.format(
                                "INSERT INTO %s VALUES (1, 'Before1'), (2, 'Before2')", tableName))
                .await();

        // Record timestamp marker
        Thread.sleep(1000);
        long timestampMarker = System.currentTimeMillis();
        Thread.sleep(1000);

        // Write phase 2 data
        tBatchEnv
                .executeSql(
                        String.format(
                                "INSERT INTO %s VALUES (3, 'After1'), (4, 'After2'), (5, 'After3')",
                                tableName))
                .await();

        // Start source from timestamp - should only read data after the marker
        FlussSource<Event> source =
                createFlussSourceWithTimestamp(DATABASE_NAME, tableName, timestampMarker);
        List<Event> allEvents = collectAllEvents(source, 4, COLLECT_TIMEOUT);
        assertCreateTableEvents(allEvents, tableName);
        List<DataChangeEvent> events = filterDataChangeEvents(allEvents);

        List<String> actual = convertToStringList(events, DataTypes.INT(), DataTypes.STRING());
        assertThat(actual)
                .containsExactlyInAnyOrder("+I[3, After1]", "+I[4, After2]", "+I[5, After3]");
    }

    @Test
    void testSavepointAndRestore(@TempDir Path tmpDir) throws Exception {
        String tableName = "savepoint_test";
        tBatchEnv
                .executeSql(
                        String.format(
                                "CREATE TABLE %s (id INT, name STRING, PRIMARY KEY (id) NOT ENFORCED)",
                                tableName))
                .await();

        // Write initial data
        tBatchEnv
                .executeSql(
                        String.format(
                                "INSERT INTO %s VALUES (1, 'A'), (2, 'B'), (3, 'C')", tableName))
                .await();

        // Phase 1: Start source job, consume data, take savepoint
        FlussSource<Event> source1 = createFlussSource(DATABASE_NAME, tableName, "earliest");
        StreamExecutionEnvironment env1 = StreamExecutionEnvironment.getExecutionEnvironment();
        env1.setParallelism(2);
        env1.enableCheckpointing(200);

        env1.fromSource(
                        source1,
                        WatermarkStrategy.noWatermarks(),
                        "FlussSource",
                        new EventTypeInfo())
                .uid("fluss-source")
                .sinkTo(new DiscardingSink<>())
                .uid("discard-sink");

        JobClient jobClient = env1.executeAsync("SavepointPhase1");
        Thread.sleep(10000); // Wait for data to be consumed and checkpoints to complete

        // Take savepoint and stop
        String savepointPath =
                jobClient
                        .stopWithSavepoint(
                                false,
                                tmpDir.toAbsolutePath().toString(),
                                SavepointFormatType.CANONICAL)
                        .get();
        LOG.info("Savepoint taken at: {}", savepointPath);

        // Write more data
        tBatchEnv
                .executeSql(String.format("INSERT INTO %s VALUES (4, 'D'), (5, 'E')", tableName))
                .await();

        // Phase 2: Restore from savepoint
        org.apache.flink.configuration.Configuration restoreConf =
                new org.apache.flink.configuration.Configuration();
        restoreConf.setString("execution.savepoint.path", savepointPath);
        // restoreConf.setString("execution.savepoint.ignore-unclaimed-state", "true");
        StreamExecutionEnvironment env2 =
                StreamExecutionEnvironment.getExecutionEnvironment(restoreConf);
        env2.setParallelism(2);
        env2.enableCheckpointing(200);

        FlussSource<Event> source2 = createFlussSource(DATABASE_NAME, tableName, "earliest");

        List<DataChangeEvent> restoredEvents = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch restoredLatch = new CountDownLatch(2);

        CloseableIterator<Event> iter =
                env2.fromSource(
                                source2,
                                WatermarkStrategy.noWatermarks(),
                                "FlussSource",
                                new EventTypeInfo())
                        .uid("fluss-source")
                        .executeAndCollect("SavepointPhase2");

        Thread restoredCollector =
                new Thread(
                        () -> {
                            try {
                                while (iter.hasNext()) {
                                    Event event = iter.next();
                                    if (event instanceof DataChangeEvent) {
                                        restoredEvents.add((DataChangeEvent) event);
                                        restoredLatch.countDown();
                                    }
                                }
                            } catch (Exception ignored) {
                            }
                        },
                        "restored-collector");
        restoredCollector.setDaemon(true);
        restoredCollector.start();

        try {
            restoredLatch.await(COLLECT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } finally {
            iter.close();
            restoredCollector.join(5000);
        }

        // After restore, should receive only the new events (D, E), not the old ones
        List<String> actual =
                convertToStringList(restoredEvents, DataTypes.INT(), DataTypes.STRING());
        assertThat(actual).containsExactlyInAnyOrder("+I[4, D]", "+I[5, E]");
    }

    @Test
    void testNewTableDiscovery() throws Exception {
        String tableA = "discover_a";
        tBatchEnv
                .executeSql(
                        String.format(
                                "CREATE TABLE %s (id INT, val STRING, PRIMARY KEY (id) NOT ENFORCED)",
                                tableA))
                .await();
        tBatchEnv
                .executeSql(String.format("INSERT INTO %s VALUES (1, 'a1'), (2, 'a2')", tableA))
                .await();

        // Start source with wildcard pattern and short discovery interval (2s)
        FlussSource<Event> source =
                createFlussSourceWithDiscoveryInterval(
                        DATABASE_NAME, "discover_*", "earliest", Duration.ofSeconds(10));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        CloseableIterator<Event> iter =
                env.fromSource(
                                source,
                                WatermarkStrategy.noWatermarks(),
                                "FlussSource",
                                new EventTypeInfo())
                        .executeAndCollect("DiscoveryTest");

        List<Event> allPhase1 = collectAllEvents(iter, 3, Duration.ofMinutes(5), false);
        assertCreateTableEvents(allPhase1, tableA);
        List<DataChangeEvent> dataChangeEvents = filterDataChangeEvents(allPhase1);
        List<String> actual =
                convertToStringList(dataChangeEvents, DataTypes.INT(), DataTypes.STRING());
        assertThat(actual).containsExactlyInAnyOrder("+I[1, a1]", "+I[2, a2]");

        // Create a new table and write data
        String tableB = "discover_b";
        tBatchEnv
                .executeSql(
                        String.format(
                                "CREATE TABLE %s (id INT, val STRING, PRIMARY KEY (id) NOT ENFORCED)",
                                tableB))
                .await();
        tBatchEnv
                .executeSql(String.format("INSERT INTO %s VALUES (1, 'b1'), (2, 'b2')", tableB))
                .await();

        // todo: 测试，将event中携带table name
        List<Event> allPhase2 = collectAllEvents(iter, 3, Duration.ofMinutes(5), true);
        assertCreateTableEvents(allPhase2, tableB);
        dataChangeEvents = filterDataChangeEvents(allPhase2);
        actual = convertToStringList(dataChangeEvents, DataTypes.INT(), DataTypes.STRING());
        assertThat(actual).containsExactlyInAnyOrder("+I[1, b1]", "+I[2, b2]");
    }

    @Test
    void testNewTableDiscoveryViaSubscriptionTable() throws Exception {
        // 1. Create the subscription table: single STRING pk column that holds the FQN
        //    (database.tableName) of each subscribed table.
        String subscriptionTable = "subscription_list";
        tBatchEnv
                .executeSql(
                        String.format(
                                "CREATE TABLE %s (table_name STRING, PRIMARY KEY (table_name) NOT ENFORCED)",
                                subscriptionTable))
                .await();

        // 2. Create target table A and seed initial subscription pointing to A.
        String tableA = "sub_discover_a";
        tBatchEnv
                .executeSql(
                        String.format(
                                "CREATE TABLE %s (id INT, val STRING, PRIMARY KEY (id) NOT ENFORCED)",
                                tableA))
                .await();
        tBatchEnv
                .executeSql(String.format("INSERT INTO %s VALUES (1, 'a1'), (2, 'a2')", tableA))
                .await();
        tBatchEnv
                .executeSql(
                        String.format(
                                "INSERT INTO %s VALUES ('%s.%s')",
                                subscriptionTable, DATABASE_NAME, tableA))
                .await();

        // 3. Start the source using FlussTableSubscriber.
        FlussSource<Event> source =
                createFlussSourceWithTableSubscriber(
                        DATABASE_NAME + "." + subscriptionTable,
                        100,
                        "earliest",
                        Duration.ofSeconds(10));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        CloseableIterator<Event> iter =
                env.fromSource(
                                source,
                                WatermarkStrategy.noWatermarks(),
                                "FlussSource",
                                new EventTypeInfo())
                        .executeAndCollect("FlussTableSubscriberDiscoveryTest");

        // Phase 1: should receive CreateTable(tableA) + its 2 data rows.
        List<Event> phase1 = collectAllEvents(iter, 3, Duration.ofMinutes(5), false);
        assertCreateTableEvents(phase1, tableA);
        List<String> actualA =
                convertToStringList(
                        filterDataChangeEvents(phase1), DataTypes.INT(), DataTypes.STRING());
        assertThat(actualA).containsExactlyInAnyOrder("+I[1, a1]", "+I[2, a2]");

        // 4. Dynamically extend the subscription: append a row pointing to a freshly
        //    created table B. The running enumerator should discover B on its next cycle.
        String tableB = "sub_discover_b";
        tBatchEnv
                .executeSql(
                        String.format(
                                "CREATE TABLE %s (id INT, val STRING, PRIMARY KEY (id) NOT ENFORCED)",
                                tableB))
                .await();
        tBatchEnv
                .executeSql(String.format("INSERT INTO %s VALUES (1, 'b1'), (2, 'b2')", tableB))
                .await();
        tBatchEnv
                .executeSql(
                        String.format(
                                "INSERT INTO %s VALUES ('%s.%s')",
                                subscriptionTable, DATABASE_NAME, tableB))
                .await();

        // Phase 2: should receive CreateTable(tableB) + its 2 data rows.
        List<Event> phase2 = collectAllEvents(iter, 3, Duration.ofMinutes(5), true);
        assertCreateTableEvents(phase2, tableB);
        List<String> actualB =
                convertToStringList(
                        filterDataChangeEvents(phase2), DataTypes.INT(), DataTypes.STRING());
        assertThat(actualB).containsExactlyInAnyOrder("+I[1, b1]", "+I[2, b2]");
    }

    @Test
    void testNewPartitionDiscovery() throws Exception {
        String tableName = "part_discover_table";
        tBatchEnv
                .executeSql(
                        String.format(
                                "CREATE TABLE %s ("
                                        + "id INT, ds STRING, val STRING, "
                                        + "PRIMARY KEY (id, ds) NOT ENFORCED"
                                        + ") PARTITIONED BY (ds)",
                                tableName))
                .await();

        // Write data to partition p1
        tBatchEnv
                .executeSql(
                        String.format(
                                "INSERT INTO %s VALUES (1, '20240101', 'p1_v1'), (2, '20240101', 'p1_v2')",
                                tableName))
                .await();

        // Start source with short discovery interval
        FlussSource<Event> source =
                createFlussSourceWithDiscoveryInterval(
                        DATABASE_NAME, tableName, "earliest", Duration.ofSeconds(10));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        CloseableIterator<Event> iter =
                env.fromSource(
                                source,
                                WatermarkStrategy.noWatermarks(),
                                "FlussSource",
                                new EventTypeInfo())
                        .executeAndCollect("PartitionDiscoveryTest");

        List<Event> allPhase1 = collectAllEvents(iter, 3, Duration.ofMinutes(5), false);
        assertCreateTableEvents(allPhase1, tableName);
        List<DataChangeEvent> dataChangeEvents = filterDataChangeEvents(allPhase1);
        List<String> actual =
                convertToStringList(
                        dataChangeEvents, DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING());
        assertThat(actual)
                .containsExactlyInAnyOrder("+I[1, 20240101, p1_v1]", "+I[2, 20240101, p1_v2]");

        // Add a new partition (p2) by inserting data with a new partition value
        tBatchEnv
                .executeSql(
                        String.format(
                                "INSERT INTO %s VALUES (3, '20240102', 'p2_v1'), (4, '20240102', 'p2_v2')",
                                tableName))
                .await();

        // Should discover and read from both partitions
        dataChangeEvents = collectEvents(iter, 2, Duration.ofMinutes(5), true);
        actual =
                convertToStringList(
                        dataChangeEvents, DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING());
        assertThat(actual)
                .containsExactlyInAnyOrder("+I[3, 20240102, p2_v1]", "+I[4, 20240102, p2_v2]");
    }

    @Test
    void testAddColumnSchemaEvolution() throws Exception {
        String tableName = "add_column_test";
        tBatchEnv
                .executeSql(
                        String.format(
                                "CREATE TABLE %s (id INT, name STRING, PRIMARY KEY (id) NOT ENFORCED)",
                                tableName))
                .await();

        // Write initial data with original schema
        tBatchEnv
                .executeSql(
                        String.format("INSERT INTO %s VALUES (1, 'Alice'), (2, 'Bob')", tableName))
                .await();

        // Start source in earliest mode with discovery interval
        FlussSource<Event> source =
                createFlussSourceWithDiscoveryInterval(
                        DATABASE_NAME, tableName, "earliest", Duration.ofSeconds(10));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        CloseableIterator<Event> iter =
                env.fromSource(
                                source,
                                WatermarkStrategy.noWatermarks(),
                                "FlussSource",
                                new EventTypeInfo())
                        .executeAndCollect("AddColumnTest");

        // Phase 1: Collect CreateTableEvent + initial 2 DataChangeEvents
        List<Event> allPhase1 = collectAllEvents(iter, 3, Duration.ofMinutes(2), false);
        assertCreateTableEvents(allPhase1, tableName);
        List<DataChangeEvent> initialEvents = filterDataChangeEvents(allPhase1);
        List<String> initialStrings =
                convertToStringList(initialEvents, DataTypes.INT(), DataTypes.STRING());
        assertThat(initialStrings).containsExactlyInAnyOrder("+I[1, Alice]", "+I[2, Bob]");

        // Phase 2: ALTER TABLE to add a new nullable column
        tBatchEnv.executeSql(String.format("ALTER TABLE %s ADD `age` INT", tableName)).await();

        // Write new data with the new column populated
        tBatchEnv
                .executeSql(
                        String.format(
                                "INSERT INTO %s VALUES (3, 'Charlie', 30), (4, 'David', 40)",
                                tableName))
                .await();

        // Phase 3: Collect remaining events — expect 1 AddColumnEvent + 2 DataChangeEvents
        List<Event> newEvents = collectAllEvents(iter, 3, Duration.ofMinutes(2), true);

        // Verify AddColumnEvent
        List<AddColumnEvent> addColumnEvents =
                newEvents.stream()
                        .filter(e -> e instanceof AddColumnEvent)
                        .map(e -> (AddColumnEvent) e)
                        .collect(Collectors.toList());
        assertThat(addColumnEvents).hasSize(1);
        AddColumnEvent addEvent = addColumnEvents.get(0);
        assertThat(addEvent.getAddedColumns()).hasSize(1);
        assertThat(addEvent.getAddedColumns().get(0).getAddColumn().getName()).isEqualTo("age");
        assertThat(addEvent.getAddedColumns().get(0).getPosition())
                .isEqualTo(AddColumnEvent.ColumnPosition.LAST);

        // Verify new DataChangeEvents have 3 fields (id, name, age)
        List<DataChangeEvent> newDataEvents =
                newEvents.stream()
                        .filter(e -> e instanceof DataChangeEvent)
                        .map(e -> (DataChangeEvent) e)
                        .collect(Collectors.toList());
        assertThat(newDataEvents).hasSize(2);
        List<String> newStrings =
                convertToStringList(
                        newDataEvents, DataTypes.INT(), DataTypes.STRING(), DataTypes.INT());
        assertThat(newStrings).containsExactlyInAnyOrder("+I[3, Charlie, 30]", "+I[4, David, 40]");
    }

    // ======================== Helper methods ========================

    private FlussSource<Event> createFlussSource(
            String database, String tablePattern, String startupMode) {
        return createFlussSourceWithDiscoveryInterval(
                database, tablePattern, startupMode, Duration.ofMinutes(1));
    }

    private FlussSource<Event> createFlussSourceWithTimestamp(
            String database, String tablePattern, long timestampMs) {
        org.apache.fluss.config.Configuration flussConfig =
                FLUSS_CLUSTER_EXTENSION.getClientConfig();
        PatternSubscriber subscriber = new PatternSubscriber(toFqnRegex(database, tablePattern));
        OffsetsInitializer offsetsInitializer = OffsetsInitializer.timestamp(timestampMs);
        return new FlussSource<>(
                subscriber,
                flussConfig,
                offsetsInitializer,
                Duration.ofMinutes(1).toMillis(),
                new FlussRecordDeserializer());
    }

    private FlussSource<Event> createFlussSourceWithTableSubscriber(
            String subscriptionTableFqn,
            int limit,
            String startupMode,
            Duration discoveryInterval) {
        org.apache.fluss.config.Configuration flussConfig =
                FLUSS_CLUSTER_EXTENSION.getClientConfig();
        FlussTableSubscriber subscriber = new FlussTableSubscriber(subscriptionTableFqn, limit);
        OffsetsInitializer offsetsInitializer;
        switch (startupMode) {
            case "earliest":
                offsetsInitializer = OffsetsInitializer.earliest();
                break;
            case "latest":
                offsetsInitializer = OffsetsInitializer.latest();
                break;
            case "full":
                offsetsInitializer = OffsetsInitializer.full();
                break;
            default:
                throw new IllegalArgumentException("Unknown startup mode: " + startupMode);
        }
        return new FlussSource<>(
                subscriber,
                flussConfig,
                offsetsInitializer,
                discoveryInterval.toMillis(),
                new FlussRecordDeserializer());
    }

    private FlussSource<Event> createFlussSourceWithDiscoveryInterval(
            String database, String tablePattern, String startupMode, Duration discoveryInterval) {
        org.apache.fluss.config.Configuration flussConfig =
                FLUSS_CLUSTER_EXTENSION.getClientConfig();
        PatternSubscriber subscriber = new PatternSubscriber(toFqnRegex(database, tablePattern));
        OffsetsInitializer offsetsInitializer;
        switch (startupMode) {
            case "earliest":
                offsetsInitializer = OffsetsInitializer.earliest();
                break;
            case "latest":
                offsetsInitializer = OffsetsInitializer.latest();
                break;
            case "full":
                offsetsInitializer = OffsetsInitializer.full();
                break;
            default:
                throw new IllegalArgumentException("Unknown startup mode: " + startupMode);
        }
        return new FlussSource<>(
                subscriber,
                flussConfig,
                offsetsInitializer,
                discoveryInterval.toMillis(),
                new FlussRecordDeserializer());
    }

    /**
     * Translates the legacy (database, tablePattern) arguments into a single Java regex matching
     * fully-qualified {@code database.tableName} names. The {@code '*'} wildcard in the old
     * tablePattern is translated to regex {@code .*}.
     */
    private static String toFqnRegex(String database, String tablePattern) {
        return java.util.regex.Pattern.quote(database) + "\\." + tablePattern.replace("*", ".*");
    }

    /**
     * Collects ALL event types from a FlussSource using executeAndCollect. Returns when
     * expectedCount events have been collected or timeout is reached.
     */
    private List<Event> collectAllEvents(
            FlussSource<Event> source, int expectedCount, Duration timeout) throws Exception {
        return collectAllEvents(source, expectedCount, timeout, 2);
    }

    private List<Event> collectAllEvents(
            FlussSource<Event> source, int expectedCount, Duration timeout, int parallelism)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        CloseableIterator<Event> iter =
                env.fromSource(
                                source,
                                WatermarkStrategy.noWatermarks(),
                                "FlussSource",
                                new EventTypeInfo())
                        .executeAndCollect("FlussSourceCollect");
        return collectAllEvents(iter, expectedCount, timeout, true);
    }

    /**
     * Collects events from a FlussSource using executeAndCollect with a background thread. Returns
     * when expectedCount events have been collected or timeout is reached.
     */
    private List<DataChangeEvent> collectEvents(
            FlussSource<Event> source, int expectedCount, Duration timeout) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        CloseableIterator<Event> iter =
                env.fromSource(
                                source,
                                WatermarkStrategy.noWatermarks(),
                                "FlussSource",
                                new EventTypeInfo())
                        .executeAndCollect("FlussSourceCollect");
        return collectEvents(iter, expectedCount, timeout, true);
    }

    private List<DataChangeEvent> collectEvents(
            CloseableIterator<Event> iter,
            int expectedCount,
            Duration timeout,
            boolean closeIterator)
            throws Exception {

        List<DataChangeEvent> events = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(expectedCount);

        Thread collector =
                new Thread(
                        () -> {
                            try {
                                while (events.size() < expectedCount && iter.hasNext()) {
                                    Event event = iter.next();
                                    if (event instanceof DataChangeEvent) {
                                        events.add((DataChangeEvent) event);
                                        latch.countDown();
                                    }
                                }
                            } catch (Exception ignored) {
                                // Iterator closed or interrupted
                                System.out.println();
                            }
                        },
                        "event-collector");
        collector.setDaemon(true);
        collector.start();

        try {
            boolean completed = latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
            if (!completed) {
                LOG.warn(
                        "Timeout collecting events. Expected {}, got {}",
                        expectedCount,
                        events.size());
            }
        } finally {
            if (closeIterator) {
                iter.close();
            }
            collector.join(5000);
        }

        return events;
    }

    /**
     * Collects ALL event types (DataChangeEvent, SchemaChangeEvent, etc.) from the iterator.
     * Returns when expectedCount events have been collected or timeout is reached.
     */
    private List<Event> collectAllEvents(
            CloseableIterator<Event> iter,
            int expectedCount,
            Duration timeout,
            boolean closeIterator)
            throws Exception {

        List<Event> events = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(expectedCount);

        Thread collector =
                new Thread(
                        () -> {
                            try {
                                while (events.size() < expectedCount && iter.hasNext()) {
                                    Event event = iter.next();
                                    events.add(event);
                                    latch.countDown();
                                }
                            } catch (Exception ignored) {
                                // Iterator closed or interrupted
                            }
                        },
                        "all-event-collector");
        collector.setDaemon(true);
        collector.start();

        try {
            boolean completed = latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
            if (!completed) {
                LOG.warn(
                        "Timeout collecting all events. Expected {}, got {}",
                        expectedCount,
                        events.size());
            }
        } finally {
            if (closeIterator) {
                iter.close();
            }
            collector.join(5000);
        }

        return events;
    }

    private void assertCreateTableEvents(List<Event> events, String... tableNames) {
        List<CreateTableEvent> createEvents =
                events.stream()
                        .filter(e -> e instanceof CreateTableEvent)
                        .map(e -> (CreateTableEvent) e)
                        .collect(Collectors.toList());
        assertThat(createEvents).hasSize(tableNames.length);
        List<String> actualTableNames =
                createEvents.stream()
                        .map(e -> e.tableId().getTableName())
                        .collect(Collectors.toList());
        assertThat(actualTableNames).containsExactlyInAnyOrder(tableNames);
    }

    private List<DataChangeEvent> filterDataChangeEvents(List<Event> events) {
        return events.stream()
                .filter(e -> e instanceof DataChangeEvent)
                .map(e -> (DataChangeEvent) e)
                .collect(Collectors.toList());
    }

    private void assertEventTableIds(
            List<DataChangeEvent> events, String database, String tableName) {
        for (DataChangeEvent event : events) {
            TableId tableId = event.tableId();
            assertThat(tableId.getSchemaName()).isEqualTo(database);
            assertThat(tableId.getTableName()).isEqualTo(tableName);
        }
    }

    /**
     * Converts a list of {@link DataChangeEvent}s to human-readable strings in the format {@code
     * +I[field1, field2, ...]} for assertions.
     */
    private List<String> convertToStringList(List<DataChangeEvent> events, DataType... fieldTypes) {
        List<RecordData.FieldGetter> fieldGetters = new ArrayList<>();
        for (int i = 0; i < fieldTypes.length; i++) {
            fieldGetters.add(RecordData.createFieldGetter(fieldTypes[i], i));
        }
        List<String> result = new ArrayList<>();
        for (DataChangeEvent event : events) {
            result.add(eventToString(event, fieldGetters));
        }
        return result;
    }

    private String eventToString(DataChangeEvent event, List<RecordData.FieldGetter> fieldGetters) {
        String prefix;
        RecordData record;
        switch (event.op()) {
            case INSERT:
                prefix = "+I";
                record = event.after();
                break;
            case DELETE:
                prefix = "-D";
                record = event.before();
                break;
            case REPLACE:
                prefix = "+R";
                record = event.after();
                break;
            case UPDATE:
                prefix = "+U";
                record = event.after();
                break;
            default:
                throw new IllegalArgumentException("Unknown op: " + event.op());
        }
        List<Object> fields = new ArrayList<>();
        for (RecordData.FieldGetter getter : fieldGetters) {
            fields.add(getter.getFieldOrNull(record));
        }
        return prefix + fields;
    }

    private void waitForFlussClusterReady() throws Exception {
        int maxRetries = 30;
        int retryIntervalMs = 1000;
        Exception lastException = null;
        for (int i = 0; i < maxRetries; i++) {
            try (Connection connection =
                    ConnectionFactory.createConnection(FLUSS_CLUSTER_EXTENSION.getClientConfig())) {
                return;
            } catch (Exception e) {
                lastException = e;
                Thread.sleep(retryIntervalMs);
            }
        }
        throw new IllegalStateException(
                "Failed to connect to Fluss cluster after " + maxRetries + " attempts",
                lastException);
    }

    private static org.apache.fluss.config.Configuration initConfig() {
        org.apache.fluss.config.Configuration conf = new org.apache.fluss.config.Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofSeconds(1));
        conf.set(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME, Duration.ofSeconds(10));
        return conf;
    }
}

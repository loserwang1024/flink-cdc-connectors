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

package org.apache.flink.cdc.pipeline.tests;

import org.apache.flink.cdc.common.test.utils.TestUtils;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.pipeline.tests.utils.PipelineTestEnvironment;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/** An End-to-end test case for Fluss pipeline connector. */
@Testcontainers
public class FlussE2eITCase extends PipelineTestEnvironment {

    private static final Logger LOG = LoggerFactory.getLogger(FlussE2eITCase.class);
    private static final Duration FLUSS_TESTCASE_TIMEOUT = Duration.ofMinutes(3);
    private static final String flussImageTag = "fluss/fluss:0.7.0";
    private static final String zooKeeperImageTag = "zookeeper:3.9.2";

    private static final List<String> flussCoordinatorProperties =
            Arrays.asList(
                    "zookeeper.address: zookeeper:2181",
                    "bind.listeners: INTERNAL://coordinator-server:0, CLIENT://coordinator-server:9123",
                    "internal.listener.name: INTERNAL",
                    "remote.data.dir: /tmp/fluss/remote-data",
                    "# security properties",
                    "security.protocol.map: CLIENT:SASL, INTERNAL:PLAINTEXT",
                    "security.sasl.enabled.mechanisms: PLAIN",
                    "security.sasl.plain.jaas.config: com.alibaba.fluss.security.auth.sasl.plain.PlainLoginModule required user_admin=\"admin-pass\" user_developer=\"developer-pass\";",
                    "#authorizer.enabled: true",
                    "super.users: User:admin");

    private static final List<String> flussTabletServerProperties =
            Arrays.asList(
                    "zookeeper.address: zookeeper:2181",
                    "bind.listeners: INTERNAL://tablet-server:0, CLIENT://tablet-server:9123",
                    "internal.listener.name: INTERNAL",
                    "tablet-server.id: 0",
                    "kv.snapshot.interval: 0s",
                    "data.dir: /tmp/fluss/data",
                    "remote.data.dir: /tmp/fluss/remote-data",
                    "# security properties",
                    "security.protocol.map: CLIENT:SASL, INTERNAL:PLAINTEXT",
                    "security.sasl.enabled.mechanisms: PLAIN",
                    "security.sasl.plain.jaas.config: com.alibaba.fluss.security.auth.sasl.plain.PlainLoginModule required user_admin=\"admin-pass\" user_developer=\"developer-pass\";",
                    "#authorizer.enabled: true",
                    "super.users: User:admin");

    @Container
    private static final GenericContainer<?> ZOOKEEPER =
            new GenericContainer<>(zooKeeperImageTag)
                    .withNetworkAliases("zookeeper")
                    .withExposedPorts(2181)
                    .withNetwork(NETWORK)
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @Container
    private static final GenericContainer<?> FLUSS_COORDINATOR =
            new GenericContainer<>(flussImageTag)
                    .withEnv(
                            ImmutableMap.of(
                                    "FLUSS_PROPERTIES",
                                    String.join("\n", flussCoordinatorProperties)))
                    .withCommand("coordinatorServer")
                    .withNetworkAliases("coordinator-server")
                    .withExposedPorts(9123)
                    .withNetwork(NETWORK)
                    .dependsOn(ZOOKEEPER)
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @Container
    private static final GenericContainer<?> FLUSS_TABLET_SERVER =
            new GenericContainer<>(flussImageTag)
                    .withEnv(
                            ImmutableMap.of(
                                    "FLUSS_PROPERTIES",
                                    String.join("\n", flussTabletServerProperties)))
                    .withCommand("tabletServer")
                    .withNetworkAliases("tablet-server")
                    .withExposedPorts(9123)
                    .withNetwork(NETWORK)
                    .dependsOn(ZOOKEEPER, FLUSS_COORDINATOR)
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    protected final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(MYSQL, "mysql_inventory", MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);

    @BeforeEach
    public void before() throws Exception {
        super.before();
        inventoryDatabase.createAndInitialize();
        jobManager.copyFileToContainer(
                MountableFile.forHostPath(TestUtils.getResource("fluss-sql-connector.jar")),
                "/tmp/fluss-sql-connector.jar");
    }

    @AfterEach
    public void after() {
        super.after();
        inventoryDatabase.dropDatabase();
    }

    @Test
    void testMySqlToFluss() throws Exception {
        String database = inventoryDatabase.getDatabaseName();
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: mysql\n"
                                + "  hostname: %s\n"
                                + "  port: 3306\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  tables: %s.\\.*\n"
                                + "  server-id: 5400-5404\n"
                                + "  server-time-zone: UTC\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: fluss\n"
                                + "  bootstrap.servers: coordinator-server:9123\n"
                                + "  properties.client.security.protocol: sasl\n"
                                + "  properties.client.security.sasl.mechanism: PLAIN\n"
                                + "  properties.client.security.sasl.username: developer\n"
                                + "  properties.client.security.sasl.password: developer-pass\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: %d",
                        INTER_CONTAINER_MYSQL_ALIAS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        inventoryDatabase.getDatabaseName(),
                        parallelism);
        Path flussConnector = TestUtils.getResource("fluss-cdc-pipeline-connector.jar");
        submitPipelineJob(pipelineJob, flussConnector);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        validateSinkResult(
                database,
                "products",
                Arrays.asList(
                        "101, One, Alice, 3.202, red, {\"key1\": \"value1\"}, null",
                        "102, Two, Bob, 1.703, white, {\"key2\": \"value2\"}, null",
                        "103, Three, Cecily, 4.105, red, {\"key3\": \"value3\"}, null",
                        "104, Four, Derrida, 1.857, white, {\"key4\": \"value4\"}, null",
                        "105, Five, Evelyn, 5.211, red, {\"K\": \"V\", \"k\": \"v\"}, null",
                        "106, Six, Ferris, 9.813, null, null, null",
                        "107, Seven, Grace, 2.117, null, null, null",
                        "108, Eight, Hesse, 6.819, null, null, null",
                        "109, Nine, IINA, 5.223, null, null, null"));

        validateSinkResult(
                database,
                "customers",
                Arrays.asList(
                        "101, user_1, Shanghai, 123567891234",
                        "102, user_2, Shanghai, 123567891234",
                        "103, user_3, Shanghai, 123567891234",
                        "104, user_4, Shanghai, 123567891234"));
    }

    private List<String> fetchFlussTableRows(String database, String table) throws Exception {
        String template =
                readLines("docker/peek-fluss.sql").stream()
                        .filter(line -> !line.startsWith("--"))
                        .collect(Collectors.joining("\n"));
        String sql = String.format(template, database, table);
        String containerSqlPath = sharedVolume.toString() + "/peek.sql";
        jobManager.copyFileToContainer(Transferable.of(sql), containerSqlPath);

        org.testcontainers.containers.Container.ExecResult result =
                jobManager.execInContainer(
                        "/opt/flink/bin/sql-client.sh",
                        "--jar",
                        "/tmp/fluss-sql-connector.jar",
                        "-f",
                        containerSqlPath);
        LOG.info(result.getStdout());
        if (result.getExitCode() != 0) {
            throw new RuntimeException(
                    "Failed to execute peek script. Stdout: "
                            + result.getStdout()
                            + "; Stderr: "
                            + result.getStderr());
        }

        return Arrays.stream(result.getStdout().split("\n"))
                .filter(line -> line.startsWith("|"))
                .skip(1)
                .map(FlussE2eITCase::extractRow)
                .map(row -> String.format("%s", String.join(", ", row)))
                .collect(Collectors.toList());
    }

    private static String[] extractRow(String row) {
        return Arrays.stream(row.split("\\|"))
                .map(String::trim)
                .filter(col -> !col.isEmpty())
                .map(col -> col.equals("<NULL>") ? "null" : col)
                .toArray(String[]::new);
    }

    private void validateSinkResult(String database, String table, List<String> expected)
            throws InterruptedException {
        LOG.info("Verifying Fluss {}::{} results...", database, table);
        long deadline = System.currentTimeMillis() + FLUSS_TESTCASE_TIMEOUT.toMillis();
        List<String> results = Collections.emptyList();
        while (System.currentTimeMillis() < deadline) {
            try {
                results = fetchFlussTableRows(database, table);
                Assertions.assertThat(results).containsExactlyInAnyOrderElementsOf(expected);
                LOG.info(
                        "Successfully verified {} records in {} seconds.",
                        expected.size(),
                        (System.currentTimeMillis() - deadline + FLUSS_TESTCASE_TIMEOUT.toMillis())
                                / 1000);
                return;
            } catch (Exception e) {
                LOG.warn("Validate failed, waiting for the next loop...", e);
            } catch (AssertionError ignored) {
                // AssertionError contains way too much records and might flood the log output.
                LOG.warn(
                        "Results mismatch, expected {} records, but got {} actually. Waiting for the next loop...",
                        expected.size(),
                        results.size());
            }
            Thread.sleep(1000L);
        }
        Assertions.assertThat(results).containsExactlyInAnyOrderElementsOf(expected);
    }
}

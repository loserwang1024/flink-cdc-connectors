/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mongodb.source;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.util.ExceptionUtils;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.ververica.cdc.connectors.mongodb.utils.MongoDBTestUtils;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.ververica.cdc.connectors.mongodb.utils.MongoDBAssertUtils.assertEqualsInAnyOrder;
import static com.ververica.cdc.connectors.mongodb.utils.MongoDBContainer.FLINK_USER;
import static com.ververica.cdc.connectors.mongodb.utils.MongoDBContainer.FLINK_USER_PASSWORD;
import static com.ververica.cdc.connectors.mongodb.utils.MongoDBTestUtils.triggerFailover;
import static java.lang.String.format;
import static org.apache.flink.util.Preconditions.checkState;

/** IT tests to cover various newly added collections during capture process. */
public class NewlyAddedTableITCase extends MongoDBSourceTestBase {

    @Rule public final Timeout timeoutPerTest = Timeout.seconds(500);

    private String customerDatabase;
    protected static final int DEFAULT_PARALLELISM = 4;

    private final ScheduledExecutorService mockWalLogExecutor = Executors.newScheduledThreadPool(1);

    @Before
    public void before() throws SQLException {
        customerDatabase = "customer_" + Integer.toUnsignedString(new Random().nextInt(), 36);
        TestValuesTableFactory.clearAllData();
        // prepare initial data for given collection
        String collectionName = "produce_changelog";
        // enable system-level fulldoc pre & post image feature
        CONTAINER.executeCommand(
                "use admin; db.runCommand({ setClusterParameter: { changeStreamOptions: { preAndPostImages: { expireAfterSeconds: 'off' } } } })");

        // mock continuous changelog during the newly added collections capturing process
        MongoDatabase mongoDatabase = mongodbClient.getDatabase(customerDatabase);
        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collectionName);
        mockWalLogExecutor.schedule(
                () -> {
                    Document document = new Document();
                    document.put("cid", 1);
                    document.put("cnt", 103L);
                    mongoCollection.insertOne(document);
                    mongoCollection.deleteOne(Filters.eq("cid", 1));
                },
                500,
                TimeUnit.MICROSECONDS);
    }

    @After
    public void after() {
        mockWalLogExecutor.shutdown();
        MongoDatabase mongoDatabase = mongodbClient.getDatabase(customerDatabase);
        mongoDatabase.drop();
    }

    @Test
    public void testNewlyAddedTableForExistsPipelineOnce() throws Exception {
        testNewlyAddedTableOneByOne(
                1,
                MongoDBTestUtils.FailoverType.NONE,
                MongoDBTestUtils.FailoverPhase.NEVER,
                false,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    public void testNewlyAddedTableForExistsPipelineOnceWithAheadBinlog() throws Exception {
        testNewlyAddedTableOneByOne(
                1,
                MongoDBTestUtils.FailoverType.NONE,
                MongoDBTestUtils.FailoverPhase.NEVER,
                true,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    public void testNewlyAddedTableForExistsPipelineTwice() throws Exception {
        testNewlyAddedTableOneByOne(
                DEFAULT_PARALLELISM,
                MongoDBTestUtils.FailoverType.NONE,
                MongoDBTestUtils.FailoverPhase.NEVER,
                false,
                "address_hangzhou",
                "address_beijing",
                "address_shanghai");
    }

    @Test
    public void testNewlyAddedTableForExistsPipelineTwiceWithAheadBinlog() throws Exception {
        testNewlyAddedTableOneByOne(
                DEFAULT_PARALLELISM,
                MongoDBTestUtils.FailoverType.NONE,
                MongoDBTestUtils.FailoverPhase.NEVER,
                true,
                "address_hangzhou",
                "address_beijing",
                "address_shanghai");
    }

    @Test
    public void testNewlyAddedTableForExistsPipelineTwiceWithAheadBinlogAndAutoCloseReader()
            throws Exception {
        Map<String, String> otherOptions = new HashMap<>();
        otherOptions.put("scan.incremental.close-idle-reader.enabled", "true");
        testNewlyAddedTableOneByOne(
                DEFAULT_PARALLELISM,
                otherOptions,
                MongoDBTestUtils.FailoverType.NONE,
                MongoDBTestUtils.FailoverPhase.NEVER,
                true,
                "address_hangzhou",
                "address_beijing",
                "address_shanghai");
    }

    @Test
    public void testNewlyAddedTableForExistsPipelineThrice() throws Exception {
        testNewlyAddedTableOneByOne(
                DEFAULT_PARALLELISM,
                MongoDBTestUtils.FailoverType.NONE,
                MongoDBTestUtils.FailoverPhase.NEVER,
                false,
                "address_hangzhou",
                "address_beijing",
                "address_shanghai",
                "address_shenzhen");
    }

    @Test
    public void testNewlyAddedTableForExistsPipelineThriceWithAheadBinlog() throws Exception {
        testNewlyAddedTableOneByOne(
                DEFAULT_PARALLELISM,
                MongoDBTestUtils.FailoverType.NONE,
                MongoDBTestUtils.FailoverPhase.NEVER,
                true,
                "address_hangzhou",
                "address_beijing",
                "address_shanghai",
                "address_shenzhen");
    }

    @Test
    public void testNewlyAddedTableForExistsPipelineSingleParallelism() throws Exception {
        testNewlyAddedTableOneByOne(
                1,
                MongoDBTestUtils.FailoverType.NONE,
                MongoDBTestUtils.FailoverPhase.NEVER,
                false,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    public void testNewlyAddedTableForExistsPipelineSingleParallelismWithAheadBinlog()
            throws Exception {
        testNewlyAddedTableOneByOne(
                1,
                MongoDBTestUtils.FailoverType.NONE,
                MongoDBTestUtils.FailoverPhase.NEVER,
                true,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    public void testJobManagerFailoverForNewlyAddedTable() throws Exception {
        testNewlyAddedTableOneByOne(
                DEFAULT_PARALLELISM,
                MongoDBTestUtils.FailoverType.JM,
                MongoDBTestUtils.FailoverPhase.SNAPSHOT,
                false,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    public void testJobManagerFailoverForNewlyAddedTableWithAheadBinlog() throws Exception {
        testNewlyAddedTableOneByOne(
                DEFAULT_PARALLELISM,
                MongoDBTestUtils.FailoverType.JM,
                MongoDBTestUtils.FailoverPhase.SNAPSHOT,
                true,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    public void testTaskManagerFailoverForNewlyAddedTable() throws Exception {
        testNewlyAddedTableOneByOne(
                1,
                MongoDBTestUtils.FailoverType.TM,
                MongoDBTestUtils.FailoverPhase.STREAM,
                false,
                "address_hangzhou",
                "address_beijing");
    }

    @Test
    public void testTaskManagerFailoverForNewlyAddedTableWithAheadBinlog() throws Exception {
        testNewlyAddedTableOneByOne(
                1,
                MongoDBTestUtils.FailoverType.TM,
                MongoDBTestUtils.FailoverPhase.STREAM,
                false,
                "address_hangzhou",
                "address_beijing");
    }

    private void testNewlyAddedTableOneByOne(
            int parallelism,
            MongoDBTestUtils.FailoverType failoverType,
            MongoDBTestUtils.FailoverPhase failoverPhase,
            boolean makeBinlogBeforeCapture,
            String... captureAddressCollections)
            throws Exception {
        testNewlyAddedTableOneByOne(
                parallelism,
                new HashMap<>(),
                failoverType,
                failoverPhase,
                makeBinlogBeforeCapture,
                captureAddressCollections);
    }

    private void testNewlyAddedTableOneByOne(
            int parallelism,
            Map<String, String> sourceOptions,
            MongoDBTestUtils.FailoverType failoverType,
            MongoDBTestUtils.FailoverPhase failoverPhase,
            boolean makeBinlogBeforeCapture,
            String... captureAddressCollections)
            throws Exception {

        // step 1: create mongodb collections with initial data
        initialAddressCollections(
                mongodbClient.getDatabase(customerDatabase), captureAddressCollections);

        final TemporaryFolder temporaryFolder = new TemporaryFolder();
        temporaryFolder.create();
        final String savepointDirectory = temporaryFolder.newFolder().toURI().toString();

        // test newly added collection one by one
        String finishedSavePointPath = null;
        List<String> fetchedDataList = new ArrayList<>();
        for (int round = 0; round < captureAddressCollections.length; round++) {
            String[] captureCollectionsThisRound =
                    Arrays.asList(captureAddressCollections)
                            .subList(0, round + 1)
                            .toArray(new String[0]);
            String newlyAddedCollection = captureAddressCollections[round];
            if (makeBinlogBeforeCapture) {
                makeBinlogBeforeCaptureForAddressCollection(
                        mongodbClient.getDatabase(customerDatabase), newlyAddedCollection);
            }
            StreamExecutionEnvironment env =
                    getStreamExecutionEnvironmentFromSavePoint(finishedSavePointPath, parallelism);
            StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

            String createTableStatement =
                    getCreateTableStatement(sourceOptions, captureCollectionsThisRound);
            tEnv.executeSql(createTableStatement);
            tEnv.executeSql(
                    "CREATE TABLE sink ("
                            + " collection_name STRING,"
                            + " cid BIGINT,"
                            + " country STRING,"
                            + " city STRING,"
                            + " detail_address STRING,"
                            + " primary key (city, cid) not enforced"
                            + ") WITH ("
                            + " 'connector' = 'values',"
                            + " 'sink-insert-only' = 'false'"
                            + ")");
            TableResult tableResult =
                    tEnv.executeSql(
                            "insert into sink select collection_name, cid, country, city, detail_address from address");
            JobClient jobClient = tableResult.getJobClient().get();

            // step 2: assert fetched snapshot data in this round
            String cityName = newlyAddedCollection.split("_")[1];
            List<String> expectedSnapshotDataThisRound =
                    Arrays.asList(
                            format(
                                    "+I[%s, 416874195632735147, China, %s, %s West Town address 1]",
                                    newlyAddedCollection, cityName, cityName),
                            format(
                                    "+I[%s, 416927583791428523, China, %s, %s West Town address 2]",
                                    newlyAddedCollection, cityName, cityName),
                            format(
                                    "+I[%s, 417022095255614379, China, %s, %s West Town address 3]",
                                    newlyAddedCollection, cityName, cityName));
            if (makeBinlogBeforeCapture) {
                expectedSnapshotDataThisRound =
                        Arrays.asList(
                                format(
                                        "+I[%s, 416874195632735147, China, %s, %s West Town address 1]",
                                        newlyAddedCollection, cityName, cityName),
                                format(
                                        "+I[%s, 416927583791428523, China, %s, %s West Town address 2]",
                                        newlyAddedCollection, cityName, cityName),
                                format(
                                        "+I[%s, 417022095255614379, China, %s, %s West Town address 3]",
                                        newlyAddedCollection, cityName, cityName),
                                format(
                                        "+I[%s, 417022095255614381, China, %s, %s West Town address 5]",
                                        newlyAddedCollection, cityName, cityName));
            }

            // trigger failover after some snapshot data read finished
            if (failoverPhase == MongoDBTestUtils.FailoverPhase.SNAPSHOT) {
                triggerFailover(
                        failoverType,
                        jobClient.getJobID(),
                        miniClusterResource.getMiniCluster(),
                        () -> sleepMs(100));
            }
            fetchedDataList.addAll(expectedSnapshotDataThisRound);
            waitForUpsertSinkSize("sink", fetchedDataList.size());
            assertEqualsInAnyOrder(fetchedDataList, TestValuesTableFactory.getResults("sink"));

            // Thread.sleep(1000);

            // step 3: make some changelog data for this round
            makeFirstPartBinlogForAddressCollection(
                    mongodbClient.getDatabase(customerDatabase), newlyAddedCollection);
            if (failoverPhase == MongoDBTestUtils.FailoverPhase.STREAM) {
                triggerFailover(
                        failoverType,
                        jobClient.getJobID(),
                        miniClusterResource.getMiniCluster(),
                        () -> sleepMs(100));
            }
            makeSecondPartBinlogForAddressCollections(
                    mongodbClient.getDatabase(customerDatabase), newlyAddedCollection);

            // step 4: assert fetched changelog data in this round

            // retract the old data with id 416874195632735147
            fetchedDataList =
                    fetchedDataList.stream()
                            .filter(
                                    r ->
                                            !r.contains(
                                                    format(
                                                            "%s, 416874195632735147",
                                                            newlyAddedCollection)))
                            .collect(Collectors.toList());
            List<String> expectedBinlogUpsertDataThisRound =
                    Arrays.asList(
                            // add the new data with id 416874195632735147
                            format(
                                    "+I[%s, 416874195632735147, CHINA, %s, %s West Town address 1]",
                                    newlyAddedCollection, cityName, cityName),
                            format(
                                    "+I[%s, 417022095255614380, China, %s, %s West Town address 4]",
                                    newlyAddedCollection, cityName, cityName));

            // step 5: assert fetched changelog data in this round
            fetchedDataList.addAll(expectedBinlogUpsertDataThisRound);

            waitForUpsertSinkSize("sink", fetchedDataList.size());
            // the result size of sink may arrive fetchedDataList.size() with old data, wait one
            // checkpoint to wait retract old record and send new record
            Thread.sleep(1000);
            assertEqualsInAnyOrder(fetchedDataList, TestValuesTableFactory.getResults("sink"));

            // step 6: trigger savepoint
            if (round != captureAddressCollections.length - 1) {
                finishedSavePointPath = triggerSavepointWithRetry(jobClient, savepointDirectory);
            }
            jobClient.cancel().get();
        }
    }

    private void initialAddressCollections(
            MongoDatabase mongoDatabase, String[] captureCustomerCollections) {
        for (String collectionName : captureCustomerCollections) {
            // make initial data for given table
            String cityName = collectionName.split("_")[1];
            // B - enable collection-level fulldoc pre & post image for change capture collection
            CONTAINER.executeCommandInDatabase(
                    String.format(
                            "db.createCollection('%s'); db.runCommand({ collMod: '%s', changeStreamPreAndPostImages: { enabled: true } })",
                            collectionName, collectionName),
                    mongoDatabase.getName());
            MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
            collection.insertOne(
                    addressDocOf(
                            416874195632735147L,
                            "China",
                            cityName,
                            cityName + " West Town address 1"));
            collection.insertOne(
                    addressDocOf(
                            416927583791428523L,
                            "China",
                            cityName,
                            cityName + " West Town address 2"));
            collection.insertOne(
                    addressDocOf(
                            417022095255614379L,
                            "China",
                            cityName,
                            cityName + " West Town address 3"));
        }
    }

    private void makeFirstPartBinlogForAddressCollection(
            MongoDatabase mongoDatabase, String collectionName) {
        MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
        collection.updateOne(
                Filters.eq("cid", 416874195632735147L), Updates.set("country", "CHINA"));
    }

    private void makeSecondPartBinlogForAddressCollections(
            MongoDatabase mongoDatabase, String collectionName) {
        String cityName = collectionName.split("_")[1];
        MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
        collection.insertOne(
                addressDocOf(
                        417022095255614380L, "China", cityName, cityName + " West Town address 4"));
    }

    private void makeBinlogBeforeCaptureForAddressCollection(
            MongoDatabase mongoDatabase, String collectionName) {
        String cityName = collectionName.split("_")[1];
        MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
        collection.insertOne(
                addressDocOf(
                        417022095255614381L, "China", cityName, cityName + " West Town address 5"));
    }

    private void sleepMs(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignored) {
        }
    }

    private String triggerSavepointWithRetry(JobClient jobClient, String savepointDirectory)
            throws ExecutionException, InterruptedException {
        int retryTimes = 0;
        // retry 600 times, it takes 100 milliseconds per time, at most retry 1 minute
        while (retryTimes < 600) {
            try {
                return jobClient.triggerSavepoint(savepointDirectory).get();
            } catch (Exception e) {
                Optional<CheckpointException> exception =
                        ExceptionUtils.findThrowable(e, CheckpointException.class);
                if (exception.isPresent()
                        && exception.get().getMessage().contains("Checkpoint triggering task")) {
                    Thread.sleep(100);
                    retryTimes++;
                } else {
                    throw e;
                }
            }
        }
        return null;
    }

    private StreamExecutionEnvironment getStreamExecutionEnvironmentFromSavePoint(
            String finishedSavePointPath, int parallelism) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        if (finishedSavePointPath != null) {
            // restore from savepoint
            // hack for test to visit protected TestStreamEnvironment#getConfiguration() method
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            Class<?> clazz =
                    classLoader.loadClass(
                            "org.apache.flink.streaming.api.environment.StreamExecutionEnvironment");
            Field field = clazz.getDeclaredField("configuration");
            field.setAccessible(true);
            Configuration configuration = (Configuration) field.get(env);
            configuration.setString(SavepointConfigOptions.SAVEPOINT_PATH, finishedSavePointPath);
        }
        env.setParallelism(parallelism);
        env.enableCheckpointing(200L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 100L));
        return env;
    }

    private String getCreateTableStatement(
            Map<String, String> otherOptions, String... captureTableNames) {
        return String.format(
                "CREATE TABLE address ("
                        + " _id STRING NOT NULL,"
                        + " collection_name STRING METADATA VIRTUAL,"
                        + " cid BIGINT NOT NULL,"
                        + " country STRING,"
                        + " city STRING,"
                        + " detail_address STRING,"
                        + " primary key (_id) not enforced"
                        + ") WITH ("
                        + " 'connector' = 'mongodb-cdc',"
                        + " 'scan.incremental.snapshot.enabled' = 'true',"
                        + " 'hosts' = '%s',"
                        + " 'username' = '%s',"
                        + " 'password' = '%s',"
                        + " 'database' = '%s',"
                        + " 'collection' = '%s',"
                        + " 'heartbeat.interval.ms' = '500',"
                        + " 'scan.full-changelog' = 'true',"
                        + " 'scan.newly-added-table.enabled' = 'true'"
                        + " %s"
                        + ")",
                CONTAINER.getHostAndPort(),
                FLINK_USER,
                FLINK_USER_PASSWORD,
                customerDatabase,
                getCollectionNameRegex(customerDatabase, captureTableNames),
                otherOptions.isEmpty()
                        ? ""
                        : ","
                                + otherOptions.entrySet().stream()
                                        .map(
                                                e ->
                                                        String.format(
                                                                "'%s'='%s'",
                                                                e.getKey(), e.getValue()))
                                        .collect(Collectors.joining(",")));
    }

    protected static void waitForUpsertSinkSize(String sinkName, int expectedSize)
            throws InterruptedException {
        while (upsertSinkSize(sinkName) < expectedSize) {
            Thread.sleep(100);
        }
    }

    protected static int upsertSinkSize(String sinkName) {
        synchronized (TestValuesTableFactory.class) {
            try {
                return TestValuesTableFactory.getResults(sinkName).size();
            } catch (IllegalArgumentException e) {
                // job is not started yet
                return 0;
            }
        }
    }

    private String getCollectionNameRegex(String database, String[] captureCustomerCollections) {
        checkState(captureCustomerCollections.length > 0);
        if (captureCustomerCollections.length == 1) {
            return captureCustomerCollections[0];
        } else {
            // pattern that matches multiple collections
            return Arrays.stream(captureCustomerCollections)
                    .map(coll -> "^(" + database + "." + coll + ")$")
                    .collect(Collectors.joining("|"));
        }
    }

    private Document addressDocOf(Long cid, String country, String city, String detailAddress) {
        Document document = new Document();
        document.put("cid", cid);
        document.put("country", country);
        document.put("city", city);
        document.put("detail_address", detailAddress);
        return document;
    }
}

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

package org.apache.flink.cdc.connectors.fluss.source.reader;

import org.apache.flink.cdc.connectors.fluss.source.split.FlussHybridSnapshotLogSplit;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussLogSplit;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussSnapshotSplit;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussSplitBase;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * A {@link SplitReader} implementation for Fluss. It reads change log records from Fluss log
 * scanners and wraps them as {@link FlussSourceRecord}s, which include the table context (table
 * path and row type) needed for downstream deserialization.
 *
 * <p>For {@link FlussHybridSnapshotLogSplit}s, it first reads the bounded KV snapshot via a {@link
 * BatchScanner}, then switches to reading change log from the log scanner. For {@link
 * FlussLogSplit}s, it reads directly from the log scanner.
 *
 * <p>For each table, a single Fluss {@link LogScanner} is shared across all bucket-level splits.
 */
public class FlussSplitReader implements SplitReader<FlussSourceRecord, FlussSplitBase> {

    private static final Logger LOG = LoggerFactory.getLogger(FlussSplitReader.class);
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);
    private static final Duration BATCH_POLL_TIMEOUT = Duration.ofMillis(10000L);

    private final Configuration flussConfig;
    private Connection connection;

    private final Map<TablePath, LogScanner> logScanners;
    private final Map<TablePath, Table> tables;
    private final Map<TablePath, RowType> tableRowTypes;
    private final Map<TableBucket, FlussSplitBase> bucketToSplit;

    // Bounded (snapshot) split reading
    private final Queue<FlussSplitBase> boundedSplits;
    @Nullable private FlussSplitBase currentBoundedSplit;
    @Nullable private BatchScanner currentBatchScanner;
    private long snapshotRecordsToSkip;
    private long currentReadRecordsCount;

    public FlussSplitReader(Configuration flussConfig) {
        this.flussConfig = flussConfig;
        this.logScanners = new HashMap<>();
        this.tables = new HashMap<>();
        this.tableRowTypes = new HashMap<>();
        this.bucketToSplit = new HashMap<>();
        this.boundedSplits = new ArrayDeque<>();
    }

    @Override
    public RecordsWithSplitIds<FlussSourceRecord> fetch() throws IOException {
        RecordsBySplits.Builder<FlussSourceRecord> builder = new RecordsBySplits.Builder<>();

        // Priority: read bounded (snapshot) splits first, then log
        checkSnapshotSplitOrStartNext();
        if (currentBatchScanner != null) {
            fetchSnapshotRecords(builder);
            return builder.build();
        }

        // Read from log scanners
        for (Map.Entry<TablePath, LogScanner> entry : logScanners.entrySet()) {
            TablePath tablePath = entry.getKey();
            LogScanner scanner = entry.getValue();
            RowType rowType = tableRowTypes.get(tablePath);

            ScanRecords scanRecords = scanner.poll(POLL_TIMEOUT);
            if (scanRecords != null && !scanRecords.isEmpty()) {
                for (TableBucket bucket : scanRecords.buckets()) {
                    FlussSplitBase split = bucketToSplit.get(bucket);
                    if (split == null) {
                        LOG.warn("Received records for unknown bucket {}, skipping", bucket);
                        continue;
                    }
                    for (ScanRecord record : scanRecords.records(bucket)) {
                        builder.add(
                                split.splitId(), new FlussSourceRecord(record, tablePath, rowType));
                    }
                }
            }
        }

        return builder.build();
    }

    @Override
    public void handleSplitsChanges(SplitsChange<FlussSplitBase> splitsChanges) {
        if (!(splitsChanges instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChanges.getClass()));
        }

        if (connection == null) {
            connection = ConnectionFactory.createConnection(flussConfig);
        }

        for (FlussSplitBase split : splitsChanges.splits()) {
            if (split.isHybridSnapshotLogSplit()) {
                FlussHybridSnapshotLogSplit hybrid = split.asHybridSnapshotLogSplit();
                // If snapshot is not finished, add to pending bounded splits
                if (!hybrid.isSnapshotFinished()) {
                    boundedSplits.add(split);
                }
                // Still need to subscribe log for after snapshot reading
                subscribeLog(split, hybrid.getLogStartingOffset());
            } else if (split.isLogSplit()) {
                subscribeLog(split, split.asLogSplit().getStartingOffset());
            } else {
                LOG.warn("Unsupported split type: {}, skipping", split.getClass().getSimpleName());
            }
        }
    }

    // -------------------------------------------------------------------------
    //  Bounded (snapshot) split reading
    // -------------------------------------------------------------------------

    /** If no bounded split is being read, poll the next one from the queue and start reading. */
    private void checkSnapshotSplitOrStartNext() {
        if (currentBatchScanner != null) {
            return;
        }

        FlussSplitBase nextSplit = boundedSplits.poll();
        if (nextSplit == null) {
            return;
        }

        currentBoundedSplit = nextSplit;
        FlussSnapshotSplit snapshotSplit = nextSplit.asSnapshotSplit();
        Table table = getOrCreateTable(nextSplit.getTablePath());
        currentBatchScanner =
                table.newScan()
                        .createBatchScanner(
                                snapshotSplit.getTableBucket(), snapshotSplit.getSnapshotId());
        snapshotRecordsToSkip = snapshotSplit.getRecordsToSkip();
        currentReadRecordsCount = 0;
        LOG.info("Started reading snapshot for split {}", nextSplit.splitId());
    }

    /**
     * Reads a batch of snapshot records. On recovery, skips records that have already been
     * processed. Each emitted record carries its cumulative {@code readRecordsCount}.
     */
    private void fetchSnapshotRecords(RecordsBySplits.Builder<FlussSourceRecord> builder)
            throws IOException {
        TablePath tablePath = currentBoundedSplit.getTablePath();
        RowType rowType = getRowType(tablePath);

        CloseableIterator<InternalRow> batch = currentBatchScanner.pollBatch(BATCH_POLL_TIMEOUT);
        if (batch == null) {
            // Snapshot fully read
            finishCurrentBoundedSplit(builder);
            return;
        }

        try {
            while (batch.hasNext()) {
                InternalRow row = batch.next();
                currentReadRecordsCount++;
                if (snapshotRecordsToSkip > 0) {
                    snapshotRecordsToSkip--;
                    continue;
                }
                ScanRecord scanRecord = new ScanRecord(row);
                builder.add(
                        currentBoundedSplit.splitId(),
                        new FlussSourceRecord(
                                scanRecord, tablePath, rowType, currentReadRecordsCount));
            }
        } finally {
            batch.close();
        }
    }

    /**
     * Called when the current bounded split's snapshot is fully read. For hybrid splits, the split
     * is NOT marked as finished since log reading continues. For pure snapshot splits, the split is
     * marked as finished.
     */
    private void finishCurrentBoundedSplit(RecordsBySplits.Builder<FlussSourceRecord> builder)
            throws IOException {
        if (currentBoundedSplit.isHybridSnapshotLogSplit()) {
            // Hybrid split: snapshot done, log reading continues — do NOT finish the split
            LOG.info("Snapshot phase finished for hybrid split {}", currentBoundedSplit.splitId());
        } else {
            // Pure snapshot split: mark as finished
            builder.addFinishedSplit(currentBoundedSplit.splitId());
            LOG.info("Snapshot split {} finished", currentBoundedSplit.splitId());
        }
        closeCurrentBoundedSplit();
    }

    private void closeCurrentBoundedSplit() throws IOException {
        try {
            if (currentBatchScanner != null) {
                currentBatchScanner.close();
            }
        } catch (Exception e) {
            throw new IOException("Failed to close batch scanner", e);
        }
        currentBatchScanner = null;
        currentBoundedSplit = null;
    }

    // -------------------------------------------------------------------------
    //  Log subscription
    // -------------------------------------------------------------------------

    private void subscribeLog(FlussSplitBase split, long startingOffset) {
        TablePath tablePath = split.getTablePath();
        ensureLogScanner(tablePath);

        LogScanner scanner = logScanners.get(tablePath);
        int bucketId = split.getTableBucket().getBucket();
        Long partitionId = split.getTableBucket().getPartitionId();
        if (startingOffset == LogScanner.EARLIEST_OFFSET) {
            if (partitionId == null) {
                scanner.subscribeFromBeginning(bucketId);
            } else {
                scanner.subscribeFromBeginning(partitionId, bucketId);
            }
        } else {
            if (partitionId == null) {
                scanner.subscribe(bucketId, startingOffset);
            } else {
                scanner.subscribe(partitionId, bucketId, startingOffset);
            }
        }

        bucketToSplit.put(split.getTableBucket(), split);
        LOG.info(
                "Subscribed bucket {} of table {} at offset {}",
                bucketId,
                split.getPhysicalTablePath(),
                startingOffset);
    }

    private void ensureLogScanner(TablePath tablePath) {
        if (!logScanners.containsKey(tablePath)) {
            Table table = getOrCreateTable(tablePath);
            LogScanner scanner = table.newScan().createLogScanner();
            logScanners.put(tablePath, scanner);
            LOG.info("Created log scanner for table {}", tablePath);
        }
    }

    protected Table getOrCreateTable(TablePath tablePath) {
        if (!tables.containsKey(tablePath)) {
            if (connection == null) {
                connection = ConnectionFactory.createConnection(flussConfig);
            }
            Table table = connection.getTable(tablePath);
            tables.put(tablePath, table);
            RowType rowType = schemaToRowType(table.getTableInfo().getSchema());
            tableRowTypes.put(tablePath, rowType);
        }
        return tables.get(tablePath);
    }

    protected RowType getRowType(TablePath tablePath) {
        return tableRowTypes.get(tablePath);
    }

    private static RowType schemaToRowType(org.apache.fluss.metadata.Schema schema) {
        List<DataField> fields = new ArrayList<>();
        for (org.apache.fluss.metadata.Schema.Column column : schema.getColumns()) {
            fields.add(new DataField(column.getName(), column.getDataType()));
        }
        return new RowType(fields);
    }

    @Override
    public void wakeUp() {}

    @Override
    public void close() throws Exception {
        if (currentBatchScanner != null) {
            try {
                currentBatchScanner.close();
            } catch (Exception e) {
                LOG.warn("Error closing batch scanner", e);
            }
        }
        for (LogScanner scanner : logScanners.values()) {
            try {
                scanner.close();
            } catch (Exception e) {
                LOG.warn("Error closing log scanner", e);
            }
        }
        for (Table table : tables.values()) {
            try {
                table.close();
            } catch (Exception e) {
                LOG.warn("Error closing table", e);
            }
        }
        if (connection != null) {
            connection.close();
        }
    }
}

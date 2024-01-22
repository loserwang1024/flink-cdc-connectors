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

package com.ververica.cdc.connectors.base.source.reader;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

import com.ververica.cdc.common.annotation.Experimental;
import com.ververica.cdc.common.annotation.VisibleForTesting;
import com.ververica.cdc.connectors.base.config.SourceConfig;
import com.ververica.cdc.connectors.base.dialect.DataSourceDialect;
import com.ververica.cdc.connectors.base.source.meta.split.ChangeEventRecords;
import com.ververica.cdc.connectors.base.source.meta.split.SnapshotSplit;
import com.ververica.cdc.connectors.base.source.meta.split.SourceRecords;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.meta.split.StreamSplit;
import com.ververica.cdc.connectors.base.source.reader.external.AbstractScanFetchTask;
import com.ververica.cdc.connectors.base.source.reader.external.FetchTask;
import com.ververica.cdc.connectors.base.source.reader.external.Fetcher;
import com.ververica.cdc.connectors.base.source.reader.external.IncrementalSourceScanFetcher;
import com.ververica.cdc.connectors.base.source.reader.external.IncrementalSourceStreamFetcher;
import com.ververica.cdc.connectors.base.source.utils.hooks.SnapshotPhaseHooks;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static com.ververica.cdc.connectors.base.source.meta.split.StreamSplit.STREAM_SPLIT_ID;

/** Basic class read {@link SourceSplitBase} and return {@link SourceRecord}. */
@Experimental
public class IncrementalSourceSplitReader<C extends SourceConfig>
        implements SplitReader<SourceRecords, SourceSplitBase> {

    private static final Logger LOG = LoggerFactory.getLogger(IncrementalSourceSplitReader.class);

    private final ArrayDeque<SnapshotSplit> snapshotSplits;
    private final ArrayDeque<StreamSplit> streamSplits;
    private final int subtaskId;

    @Nullable private Fetcher<SourceRecords, SourceSplitBase> currentFetcher;

    @Nullable private IncrementalSourceScanFetcher reusedScanFetcher;
    @Nullable private IncrementalSourceStreamFetcher reusedStreamFetcher;

    private FetchTask.Context taskContext;

    /** when suspend reading binlog and then read snapshot split. */
    private volatile boolean resumeStreamFetcherAfterSnapshotRead = false;

    @Nullable private String currentSplitId;
    private final DataSourceDialect<C> dataSourceDialect;
    private final C sourceConfig;

    private final IncrementalSourceReaderContext context;
    private final SnapshotPhaseHooks snapshotHooks;

    public IncrementalSourceSplitReader(
            int subtaskId,
            DataSourceDialect<C> dataSourceDialect,
            C sourceConfig,
            IncrementalSourceReaderContext context,
            SnapshotPhaseHooks snapshotHooks) {
        this.subtaskId = subtaskId;
        this.snapshotSplits = new ArrayDeque<>();
        this.streamSplits = new ArrayDeque<>(1);
        this.dataSourceDialect = dataSourceDialect;
        this.sourceConfig = sourceConfig;
        this.context = context;
        this.snapshotHooks = snapshotHooks;
    }

    @Override
    public RecordsWithSplitIds<SourceRecords> fetch() throws IOException {

        try {
            suspendStreamReaderIfNeed();
            return pollSplitRecords();
        } catch (Exception e) {
            LOG.warn("fetch data failed.", e);
            throw new IOException(e);
        }
    }

    /** Suspends binlog reader until updated binlog split join again. */
    private void suspendStreamReaderIfNeed() throws Exception {
        if (currentFetcher != null
                && currentFetcher instanceof IncrementalSourceStreamFetcher
                && context.isStreamSplitReaderSuspended()
                && !currentFetcher.isFinished()) {
            ((IncrementalSourceStreamFetcher) currentFetcher).stopReadTask();
            LOG.info("Suspend binlog reader to wait the binlog split update.");
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<SourceSplitBase> splitsChanges) {
        if (!(splitsChanges instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChanges.getClass()));
        }

        LOG.debug("Handling split change {}", splitsChanges);
        for (SourceSplitBase split : splitsChanges.splits()) {
            if (split.isSnapshotSplit()) {
                snapshotSplits.add(split.asSnapshotSplit());
            } else {
                streamSplits.add(split.asStreamSplit());
            }
        }
    }

    @Override
    public void wakeUp() {}

    @Override
    public void close() throws Exception {
        closeScanFetcher();
        closeStreamFetcher();
    }

    private ChangeEventRecords pollSplitRecords() throws InterruptedException {
        Iterator<SourceRecords> dataIt = null;
        if (currentFetcher == null) {
            // (1) Reads binlog split firstly and then read snapshot split
            if (streamSplits.size() > 0) {
                // the binlog split may come from:
                // (a) the initial binlog split
                // (b) added back binlog-split in newly added table process
                StreamSplit nextSplit = streamSplits.poll();
                currentSplitId = nextSplit.splitId();
                currentFetcher = getStreamFetcher();
                FetchTask<SourceSplitBase> fetchTask = dataSourceDialect.createFetchTask(nextSplit);
                currentFetcher.submitTask(fetchTask);
            } else if (snapshotSplits.size() > 0) {
                SnapshotSplit nextSplit = snapshotSplits.poll();
                currentSplitId = nextSplit.splitId();
                currentFetcher = getScanFetcher();
                FetchTask<SourceSplitBase> fetchTask = dataSourceDialect.createFetchTask(nextSplit);
                ((AbstractScanFetchTask) fetchTask).setSnapshotPhaseHooks(snapshotHooks);
                currentFetcher.submitTask(fetchTask);
            } else {
                LOG.info("No available split to read.");
            }
            dataIt = currentFetcher.pollSplitRecords();
            return dataIt == null ? finishedSplit() : forRecords(dataIt);
        } else if (currentFetcher instanceof IncrementalSourceScanFetcher) {
            // (2) try to switch to binlog split reading util current snapshot split finished
            dataIt = currentFetcher.pollSplitRecords();
            if (dataIt != null) {
                // first fetch data of snapshot split, return and emit the records of snapshot split
                ChangeEventRecords records;
                if (context.isHasAssignedBinlogSplit()) {
                    records = forNewAddedTableFinishedSplit(currentSplitId, dataIt);
                    closeScanFetcher();
                    closeStreamFetcher();
                } else {
                    records = forRecords(dataIt);
                    SnapshotSplit nextSplit = snapshotSplits.poll();
                    if (nextSplit != null) {
                        currentSplitId = nextSplit.splitId();
                        FetchTask<SourceSplitBase> fetchTask =
                                dataSourceDialect.createFetchTask(nextSplit);
                        ((AbstractScanFetchTask) fetchTask).setSnapshotPhaseHooks(snapshotHooks);
                        currentFetcher.submitTask(fetchTask);
                    } else {
                        closeScanFetcher();
                    }
                }
                return records;
            } else {
                return finishedSplit();
            }
        } else if (currentFetcher instanceof IncrementalSourceStreamFetcher) {
            // (3) switch to snapshot split reading if there are newly added snapshot splits
            dataIt = currentFetcher.pollSplitRecords();
            if (dataIt != null) {
                // try to switch to read snapshot split if there are new added snapshot
                SnapshotSplit nextSplit = snapshotSplits.poll();
                if (nextSplit != null) {
                    closeStreamFetcher();
                    LOG.info("It's turn to switch next fetch reader to snapshot split reader");
                    currentSplitId = nextSplit.splitId();
                    currentFetcher = getScanFetcher();
                    FetchTask<SourceSplitBase> fetchTask =
                            dataSourceDialect.createFetchTask(nextSplit);
                    currentFetcher.submitTask(fetchTask);
                }
                return ChangeEventRecords.forRecords(STREAM_SPLIT_ID, dataIt);
            } else {
                // null will be returned after receiving suspend binlog event
                // finish current binlog split reading
                closeStreamFetcher();
                return finishedSplit();
            }
        } else {
            throw new IllegalStateException("Unsupported reader type.");
        }
    }

    @VisibleForTesting
    public boolean canAssignNextSplit() {
        return currentFetcher == null || currentFetcher.isFinished();
    }

    private ChangeEventRecords finishedSplit() {
        final ChangeEventRecords finishedRecords =
                ChangeEventRecords.forFinishedSplit(currentSplitId);
        currentSplitId = null;
        return finishedRecords;
    }

    /**
     * Finishes new added snapshot split, mark the binlog split as finished too, we will add the
     * binlog split back in {@code MySqlSourceReader}.
     */
    private ChangeEventRecords forNewAddedTableFinishedSplit(
            final String splitId, final Iterator<SourceRecords> recordsForSplit) {
        final Set<String> finishedSplits = new HashSet<>();
        finishedSplits.add(splitId);
        finishedSplits.add(STREAM_SPLIT_ID);
        currentSplitId = null;
        return new ChangeEventRecords(splitId, recordsForSplit, finishedSplits);
    }

    private ChangeEventRecords forRecords(Iterator<SourceRecords> dataIt) {
        if (currentFetcher instanceof IncrementalSourceScanFetcher) {
            final ChangeEventRecords finishedRecords =
                    ChangeEventRecords.forSnapshotRecords(currentSplitId, dataIt);
            closeScanFetcher();
            return finishedRecords;
        } else {
            return ChangeEventRecords.forRecords(currentSplitId, dataIt);
        }
    }

    private IncrementalSourceScanFetcher getScanFetcher() {
        if (reusedScanFetcher == null) {
            reusedScanFetcher = new IncrementalSourceScanFetcher(getContext(), subtaskId);
        }
        return reusedScanFetcher;
    }

    private IncrementalSourceStreamFetcher getStreamFetcher() {
        if (reusedStreamFetcher == null) {
            reusedStreamFetcher = new IncrementalSourceStreamFetcher(getContext(), subtaskId);
        }
        return reusedStreamFetcher;
    }

    private FetchTask.Context getContext() {
        //        if (taskContext == null) {
        //            taskContext = dataSourceDialect.createFetchTaskContext(sourceConfig);
        //        }
        // todo: 思考如何复用，现在如果stream切换snapshot时，由于slotname采用global slot, 会冲突
        taskContext = dataSourceDialect.createFetchTaskContext(sourceConfig);
        return taskContext;
    }

    private void closeScanFetcher() {
        if (reusedScanFetcher != null) {
            LOG.debug("Close snapshot reader {}", reusedScanFetcher.getClass().getCanonicalName());
            reusedScanFetcher.close();
            if (currentFetcher == reusedScanFetcher) {
                currentFetcher = null;
            }
            reusedScanFetcher = null;
        }
    }

    private void closeStreamFetcher() {
        if (reusedStreamFetcher != null) {
            LOG.debug("Close binlog reader {}", reusedStreamFetcher.getClass().getCanonicalName());
            reusedStreamFetcher.close();
            if (currentFetcher == reusedStreamFetcher) {
                currentFetcher = null;
            }
            reusedStreamFetcher = null;
        }
    }
}

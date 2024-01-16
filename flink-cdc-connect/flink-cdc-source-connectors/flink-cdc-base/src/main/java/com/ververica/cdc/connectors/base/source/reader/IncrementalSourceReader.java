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

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import com.ververica.cdc.common.annotation.Experimental;
import com.ververica.cdc.connectors.base.config.SourceConfig;
import com.ververica.cdc.connectors.base.dialect.DataSourceDialect;
import com.ververica.cdc.connectors.base.source.meta.events.FinishedSnapshotSplitsAckEvent;
import com.ververica.cdc.connectors.base.source.meta.events.FinishedSnapshotSplitsReportEvent;
import com.ververica.cdc.connectors.base.source.meta.events.FinishedSnapshotSplitsRequestEvent;
import com.ververica.cdc.connectors.base.source.meta.events.LatestFinishedSplitsNumberEvent;
import com.ververica.cdc.connectors.base.source.meta.events.LatestFinishedSplitsNumberRequestEvent;
import com.ververica.cdc.connectors.base.source.meta.events.StreamSplitMetaEvent;
import com.ververica.cdc.connectors.base.source.meta.events.StreamSplitMetaRequestEvent;
import com.ververica.cdc.connectors.base.source.meta.events.StreamSplitUpdateAckEvent;
import com.ververica.cdc.connectors.base.source.meta.events.StreamSplitUpdateRequestEvent;
import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.base.source.meta.split.FinishedSnapshotSplitInfo;
import com.ververica.cdc.connectors.base.source.meta.split.SnapshotSplit;
import com.ververica.cdc.connectors.base.source.meta.split.SnapshotSplitState;
import com.ververica.cdc.connectors.base.source.meta.split.SourceRecords;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitSerializer;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitState;
import com.ververica.cdc.connectors.base.source.meta.split.StreamSplit;
import com.ververica.cdc.connectors.base.source.meta.split.StreamSplitState;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.ververica.cdc.connectors.base.source.meta.split.StreamSplit.STREAM_SPLIT_ID;
import static com.ververica.cdc.connectors.base.source.meta.split.StreamSplit.toNormalStreamSplit;
import static com.ververica.cdc.connectors.base.source.meta.split.StreamSplit.toSuspendedStreamSplit;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The multi-parallel source reader for table snapshot phase from {@link SnapshotSplit} and then
 * single-parallel source reader for table stream phase from {@link StreamSplit}.
 */
@Experimental
public class IncrementalSourceReader<T, C extends SourceConfig>
        extends SingleThreadMultiplexSourceReaderBase<
                SourceRecords, T, SourceSplitBase, SourceSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(IncrementalSourceReader.class);

    /** todo: description suspend split: which. */
    private final Map<String, SnapshotSplit> finishedUnackedSplits;
    /** todo: description suspend split: which. */
    private final Map<String, StreamSplit> uncompletedStreamSplits;

    /** todo: description suspend split: which. */
    private volatile StreamSplit suspendedStreamSplit;

    private final int subtaskId;
    private final SourceSplitSerializer sourceSplitSerializer;
    private final C sourceConfig;
    private final DataSourceDialect<C> dialect;

    private final IncrementalSourceReaderContext incrementalSourceReaderContext;

    public IncrementalSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<SourceRecords>> elementQueue,
            Supplier<IncrementalSourceSplitReader<C>> splitReaderSupplier,
            RecordEmitter<SourceRecords, T, SourceSplitState> recordEmitter,
            Configuration config,
            IncrementalSourceReaderContext incrementalSourceReaderContext,
            C sourceConfig,
            SourceSplitSerializer sourceSplitSerializer,
            DataSourceDialect<C> dialect) {
        super(
                elementQueue,
                new SingleThreadFetcherManager<>(elementQueue, splitReaderSupplier::get),
                recordEmitter,
                config,
                incrementalSourceReaderContext.getSourceReaderContext());
        this.sourceConfig = sourceConfig;
        this.finishedUnackedSplits = new HashMap<>();
        this.uncompletedStreamSplits = new HashMap<>();
        this.subtaskId = context.getIndexOfSubtask();
        this.sourceSplitSerializer = checkNotNull(sourceSplitSerializer);
        this.dialect = dialect;
        this.incrementalSourceReaderContext = incrementalSourceReaderContext;
        this.suspendedStreamSplit = null;
    }

    @Override
    public void start() {
        if (getNumberOfCurrentlyAssignedSplits() <= 1) {
            context.sendSplitRequest();
        }
    }

    @Override
    protected SourceSplitState initializedState(SourceSplitBase split) {
        if (split.isSnapshotSplit()) {
            return new SnapshotSplitState(split.asSnapshotSplit());
        } else {
            return new StreamSplitState(split.asStreamSplit());
        }
    }

    @Override
    public List<SourceSplitBase> snapshotState(long checkpointId) {
        // unfinished splits
        List<SourceSplitBase> stateSplits = super.snapshotState(checkpointId);

        // add finished snapshot splits that didn't receive ack yet
        stateSplits.addAll(finishedUnackedSplits.values());

        // add stream splits who are uncompleted
        stateSplits.addAll(uncompletedStreamSplits.values());

        return stateSplits;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // dialect.notifyCheckpointComplete(checkpointId);
    }

    @Override
    protected void onSplitFinished(Map<String, SourceSplitState> finishedSplitIds) {
        boolean requestNextSplit = true;
        if (isNewlyAddedTableSplitAndBinlogSplit(finishedSplitIds)) {
            SourceSplitState mySqlBinlogSplitState = finishedSplitIds.remove(STREAM_SPLIT_ID);
            finishedSplitIds
                    .values()
                    .forEach(
                            newAddedSplitState ->
                                    finishedUnackedSplits.put(
                                            newAddedSplitState.toSourceSplit().splitId(),
                                            newAddedSplitState
                                                    .asSnapshotSplitState()
                                                    .toSourceSplit()));
            Preconditions.checkState(finishedSplitIds.values().size() == 1);
            LOG.info(
                    "Source reader {} finished binlog split and snapshot split {}",
                    subtaskId,
                    finishedSplitIds.values().iterator().next().toSourceSplit().splitId());
            this.addSplits(
                    Collections.singletonList(
                            mySqlBinlogSplitState.asStreamSplitState().toSourceSplit()));
        } else {
            Preconditions.checkState(finishedSplitIds.size() == 1);

            for (SourceSplitState splitState : finishedSplitIds.values()) {
                SourceSplitBase sourceSplit = splitState.toSourceSplit();
                if (sourceSplit.isStreamSplit()) {
                    // Two possibilities that finish a binlog split:
                    //
                    // 1. Binlog reader is suspended by enumerator because new tables have been
                    // finished its snapshot reading.
                    // Under this case mySqlSourceReaderContext.isBinlogSplitReaderSuspended() is
                    // true and need to request the latest finished splits number.
                    //
                    // 2. Binlog reader reaches the ending offset of the split. We need to do
                    // nothing under this case.
                    if (incrementalSourceReaderContext.isStreamSplitReaderSuspended()) {
                        suspendedStreamSplit = toSuspendedStreamSplit(sourceSplit.asStreamSplit());
                        LOG.info(
                                "Source reader {} suspended binlog split reader success after the newly added table process, current offset {}",
                                subtaskId,
                                suspendedStreamSplit.getStartingOffset());
                        context.sendSourceEventToCoordinator(
                                new LatestFinishedSplitsNumberRequestEvent());
                        // do not request next split when the reader is suspended
                        requestNextSplit = false;
                    }
                } else {
                    LOG.info(
                            String.format(
                                    "Only snapshot split could finish, but the actual split is stream split %s",
                                    sourceSplit));
                    finishedUnackedSplits.put(sourceSplit.splitId(), sourceSplit.asSnapshotSplit());
                }
            }

            // todo: 这里为何不在外面report, isNewlyAddedTableSplitAndBinlogSplit也有可能啊
            reportFinishedSnapshotSplitsIfNeed();
        }
        if (requestNextSplit) {
            context.sendSplitRequest();
        }
    }

    /**
     * During the newly added table process, for the source reader who holds the binlog split, we
     * return the latest finished snapshot split and binlog split as well, this design let us have
     * opportunity to exchange binlog reading and snapshot reading, we put the binlog split back.
     */
    private boolean isNewlyAddedTableSplitAndBinlogSplit(
            Map<String, SourceSplitState> finishedSplitIds) {
        return finishedSplitIds.containsKey(STREAM_SPLIT_ID) && finishedSplitIds.size() == 2;
    }

    @Override
    public void addSplits(List<SourceSplitBase> splits) {
        // restore for finishedUnackedSplits
        List<SourceSplitBase> unfinishedSplits = new ArrayList<>();
        for (SourceSplitBase split : splits) {
            if (split.isSnapshotSplit()) {
                SnapshotSplit snapshotSplit = split.asSnapshotSplit();
                if (snapshotSplit.isSnapshotReadFinished()) {
                    finishedUnackedSplits.put(snapshotSplit.splitId(), snapshotSplit);
                } else {
                    unfinishedSplits.add(split);
                }
            } else {
                // Try to discovery table schema once for newly added tables when source reader
                // start or restore
                boolean checkNewlyAddedTableSchema =
                        !incrementalSourceReaderContext.isHasAssignedBinlogSplit()
                                && sourceConfig.isScanNewlyAddedTableEnabled();
                incrementalSourceReaderContext.setHasAssignedBinlogSplit(true);

                // the binlog split is suspended
                StreamSplit streamSplit = split.asStreamSplit();
                if (streamSplit.isSuspended()) {
                    suspendedStreamSplit = streamSplit;
                } else if (!streamSplit.isCompletedSplit()) {
                    // the stream split is uncompleted
                    uncompletedStreamSplits.put(split.splitId(), split.asStreamSplit());
                    requestStreamSplitMetaIfNeeded(split.asStreamSplit());
                } else {
                    uncompletedStreamSplits.remove(split.splitId());
                    streamSplit =
                            discoverTableSchemasForStreamSplit(
                                    streamSplit, checkNewlyAddedTableSchema);
                    unfinishedSplits.add(streamSplit);
                }
            }
        }
        // notify split enumerator again about the finished unacked snapshot splits
        reportFinishedSnapshotSplitsIfNeed();
        // add all un-finished splits (including stream split) to SourceReaderBase
        super.addSplits(unfinishedSplits);
    }

    private StreamSplit discoverTableSchemasForStreamSplit(
            StreamSplit split, boolean checkNewlyAddedTableSchema) {
        final String splitId = split.splitId();
        if (split.getTableSchemas().isEmpty() || checkNewlyAddedTableSchema) {
            Map<TableId, TableChanges.TableChange> tableSchemas;
            try {
                Map<TableId, TableChanges.TableChange> existTableSchemas = split.getTableSchemas();
                tableSchemas = dialect.discoverDataCollectionSchemas(sourceConfig);
                tableSchemas.putAll(existTableSchemas);
                LOG.info("The table schema discovery for stream split {} success", splitId);
                return StreamSplit.fillTableSchemas(split, tableSchemas);
            } catch (Exception e) {
                LOG.error("Failed to obtains table schemas due to {}", e.getMessage());
                throw new FlinkRuntimeException(e);
            }
        } else {
            LOG.warn(
                    "The stream split {} has table schemas yet, skip the table schema discovery",
                    split);
            return split;
        }
    }

    private Set<String> getExistedSplitsOfLastGroup(
            List<FinishedSnapshotSplitInfo> finishedSnapshotSplits, int metaGroupSize) {
        int splitsNumOfLastGroup =
                finishedSnapshotSplits.size() % sourceConfig.getSplitMetaGroupSize();
        if (splitsNumOfLastGroup != 0) {
            int lastGroupStart =
                    ((int) (finishedSnapshotSplits.size() / sourceConfig.getSplitMetaGroupSize()))
                            * metaGroupSize;
            // Keep same order with MySqlHybridSplitAssigner.createBinlogSplit() to avoid
            // 'invalid request meta group id' error
            List<String> sortedFinishedSnapshotSplits =
                    finishedSnapshotSplits.stream()
                            .map(FinishedSnapshotSplitInfo::getSplitId)
                            .sorted()
                            .collect(Collectors.toList());
            return new HashSet<>(
                    sortedFinishedSnapshotSplits.subList(
                            lastGroupStart, lastGroupStart + splitsNumOfLastGroup));
        }
        return new HashSet<>();
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        if (sourceEvent instanceof FinishedSnapshotSplitsAckEvent) {
            FinishedSnapshotSplitsAckEvent ackEvent = (FinishedSnapshotSplitsAckEvent) sourceEvent;
            LOG.debug(
                    "The subtask {} receives ack event for {} from enumerator.",
                    subtaskId,
                    ackEvent.getFinishedSplits());
            for (String splitId : ackEvent.getFinishedSplits()) {
                this.finishedUnackedSplits.remove(splitId);
            }
        } else if (sourceEvent instanceof FinishedSnapshotSplitsRequestEvent) {
            // report finished snapshot splits
            LOG.debug(
                    "The subtask {} receives request to report finished snapshot splits.",
                    subtaskId);
            reportFinishedSnapshotSplitsIfNeed();
        } else if (sourceEvent instanceof StreamSplitMetaEvent) {
            LOG.debug(
                    "The subtask {} receives stream meta with group id {}.",
                    subtaskId,
                    ((StreamSplitMetaEvent) sourceEvent).getMetaGroupId());
            fillMetaDataForStreamSplit((StreamSplitMetaEvent) sourceEvent);
        } else if (sourceEvent instanceof StreamSplitUpdateRequestEvent) {
            suspendStreamSplitReader();
        } else if (sourceEvent instanceof LatestFinishedSplitsNumberEvent) {
            updateStreamSplit((LatestFinishedSplitsNumberEvent) sourceEvent);
        } else {
            super.handleSourceEvents(sourceEvent);
        }
    }

    private void suspendStreamSplitReader() {
        incrementalSourceReaderContext.suspendStreamSplitReader();
    }

    private void fillMetaDataForStreamSplit(StreamSplitMetaEvent metadataEvent) {
        StreamSplit streamSplit = uncompletedStreamSplits.get(metadataEvent.getSplitId());
        if (streamSplit != null) {
            final int receivedMetaGroupId = metadataEvent.getMetaGroupId();
            final int expectedMetaGroupId =
                    getNextMetaGroupId(
                            streamSplit.getFinishedSnapshotSplitInfos().size(),
                            sourceConfig.getSplitMetaGroupSize());
            if (receivedMetaGroupId == expectedMetaGroupId) {
                Set<String> existedSplitsOfLastGroup =
                        getExistedSplitsOfLastGroup(
                                streamSplit.getFinishedSnapshotSplitInfos(),
                                sourceConfig.getSplitMetaGroupSize());

                List<FinishedSnapshotSplitInfo> newAddedMetadataGroup =
                        metadataEvent.getMetaGroup().stream()
                                .map(sourceSplitSerializer::deserialize)
                                .filter(r -> !existedSplitsOfLastGroup.contains(r.getSplitId()))
                                .collect(Collectors.toList());
                uncompletedStreamSplits.put(
                        streamSplit.splitId(),
                        StreamSplit.appendFinishedSplitInfos(streamSplit, newAddedMetadataGroup));

                LOG.info("Fill metadata of group {} to stream split", newAddedMetadataGroup.size());
            } else {
                LOG.warn(
                        "Received out of oder metadata event for split {}, the received meta group id is {}, but expected is {}, ignore it",
                        metadataEvent.getSplitId(),
                        receivedMetaGroupId,
                        expectedMetaGroupId);
            }
            requestStreamSplitMetaIfNeeded(uncompletedStreamSplits.get(streamSplit.splitId()));
        } else {
            LOG.warn(
                    "Received metadata event for split {}, but the uncompleted split map does not contain it",
                    metadataEvent.getSplitId());
        }
    }

    private void requestStreamSplitMetaIfNeeded(StreamSplit streamSplit) {
        final String splitId = streamSplit.splitId();
        if (!streamSplit.isCompletedSplit()) {
            final int nextMetaGroupId =
                    getNextMetaGroupId(
                            streamSplit.getFinishedSnapshotSplitInfos().size(),
                            sourceConfig.getSplitMetaGroupSize());
            StreamSplitMetaRequestEvent splitMetaRequestEvent =
                    new StreamSplitMetaRequestEvent(splitId, nextMetaGroupId);
            context.sendSourceEventToCoordinator(splitMetaRequestEvent);
        } else {
            LOG.info("The meta of stream split {} has been collected success", splitId);
            this.addSplits(Collections.singletonList(streamSplit));
        }
    }

    private void updateStreamSplit(LatestFinishedSplitsNumberEvent sourceEvent) {
        if (suspendedStreamSplit != null) {
            final int finishedSplitsSize = sourceEvent.getLatestFinishedSplitsNumber();
            final StreamSplit binlogSplit =
                    toNormalStreamSplit(suspendedStreamSplit, finishedSplitsSize);
            suspendedStreamSplit = null;
            // todo: 如果add back，一执行岂不是又要暂停？
            this.addSplits(Collections.singletonList(binlogSplit));

            context.sendSourceEventToCoordinator(new StreamSplitUpdateAckEvent());
            LOG.info(
                    "Source reader {} notifies enumerator that binlog split has been updated.",
                    subtaskId);

            incrementalSourceReaderContext.wakeupSuspendedBinlogSplitReader();
            LOG.info(
                    "Source reader {} wakes up suspended binlog reader as binlog split has been updated.",
                    subtaskId);
        } else {
            LOG.warn("Unexpected event {}, this should not happen.", sourceEvent);
        }
    }

    private void reportFinishedSnapshotSplitsIfNeed() {
        if (!finishedUnackedSplits.isEmpty()) {
            final Map<String, Offset> finishedOffsets = new HashMap<>();
            for (SnapshotSplit split : finishedUnackedSplits.values()) {
                finishedOffsets.put(split.splitId(), split.getHighWatermark());
            }
            FinishedSnapshotSplitsReportEvent reportEvent =
                    new FinishedSnapshotSplitsReportEvent(finishedOffsets);
            context.sendSourceEventToCoordinator(reportEvent);
            LOG.debug(
                    "The subtask {} reports offsets of finished snapshot splits {}.",
                    subtaskId,
                    finishedOffsets);
        }
    }

    /** Returns next meta group id according to received meta number and meta group size. */
    public static int getNextMetaGroupId(int receivedMetaNum, int metaGroupSize) {
        Preconditions.checkState(metaGroupSize > 0);
        return receivedMetaNum / metaGroupSize;
    }

    @Override
    protected SourceSplitBase toSplitType(String splitId, SourceSplitState splitState) {
        return splitState.toSourceSplit();
    }
}

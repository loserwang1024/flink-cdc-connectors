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

package com.ververica.cdc.connectors.postgres.source.reader;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import com.ververica.cdc.connectors.base.config.SourceConfig;
import com.ververica.cdc.connectors.base.dialect.DataSourceDialect;
import com.ververica.cdc.connectors.base.source.assigner.AssignerStatus;
import com.ververica.cdc.connectors.base.source.meta.split.SnapshotSplit;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitSerializer;
import com.ververica.cdc.connectors.base.source.meta.split.StreamSplit;
import com.ververica.cdc.connectors.base.source.reader.IncrementalSourceReader;
import com.ververica.cdc.connectors.base.source.reader.IncrementalSourceReaderContext;
import com.ververica.cdc.connectors.postgres.source.events.ReportCommitSuspendEvent;
import com.ververica.cdc.connectors.postgres.source.events.SyncAssignStatusEvent;

import java.util.function.Supplier;

import static com.ververica.cdc.connectors.base.source.assigner.AssignerStatus.isNewlyAddedAssigning;
import static com.ververica.cdc.connectors.base.source.assigner.AssignerStatus.isNewlyAddedAssigningFinished;
import static com.ververica.cdc.connectors.base.source.assigner.AssignerStatus.isNewlyAddedAssigningSnapshotFinished;

/**
 * The multi-parallel postgres source reader for table snapshot phase from {@link SnapshotSplit} and
 * then single-parallel source reader for table stream phase from {@link StreamSplit}.
 */
public class PostgresSourceReader extends IncrementalSourceReader {

    /** whether to commit offset. */
    private volatile boolean isCommitOffset = false;

    public PostgresSourceReader(
            FutureCompletingBlockingQueue elementQueue,
            Supplier supplier,
            RecordEmitter recordEmitter,
            Configuration config,
            IncrementalSourceReaderContext incrementalSourceReaderContext,
            SourceConfig sourceConfig,
            SourceSplitSerializer sourceSplitSerializer,
            DataSourceDialect dialect) {
        super(
                elementQueue,
                supplier,
                recordEmitter,
                config,
                incrementalSourceReaderContext,
                sourceConfig,
                sourceSplitSerializer,
                dialect);
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        if (sourceEvent instanceof SyncAssignStatusEvent) {
            AssignerStatus status =
                    AssignerStatus.fromStatusCode(
                            ((SyncAssignStatusEvent) sourceEvent).getAssignStatusCode());
            // In the following situations, do not commit offset:
            // 1. Assign status is INITIAL_ASSIGNING and NEWLY_ADDED_ASSIGNING_FINISHED
            // 2. Assign status is NEWLY_ADDED_ASSIGNING_FINISHED, but stream split is still not
            // added back.
            if (isNewlyAddedAssigning(status)
                    || isNewlyAddedAssigningSnapshotFinished(status)
                    || (isNewlyAddedAssigningFinished(status) & isStreamSplitSuspend())) {
                isCommitOffset = false;
            } else {
                isCommitOffset = true;
            }
            context.sendSourceEventToCoordinator(new ReportCommitSuspendEvent(isCommitOffset));
        } else {
            super.handleSourceEvents(sourceEvent);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // Only if scan newly added table is enable, offset commit is controlled by isCommitOffset.
        if (!sourceConfig.isScanNewlyAddedTableEnabled() || isCommitOffset) {
            super.notifyCheckpointComplete(checkpointId);
        }
    }

    private boolean isStreamSplitSuspend() {
        return suspendedStreamSplit == null || uncompletedStreamSplits.isEmpty();
    }
}

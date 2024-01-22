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
import com.ververica.cdc.connectors.postgres.source.events.SyncAssignStatus;
import com.ververica.cdc.connectors.postgres.source.events.SyncAssignStatusAck;

import java.util.function.Supplier;

/**
 * todo: The multi-parallel source reader for table snapshot phase from {@link SnapshotSplit} and
 * then single-parallel source reader for table stream phase from {@link StreamSplit}.
 */
public class PostgresSourceReader extends IncrementalSourceReader {

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
        if (sourceEvent instanceof SyncAssignStatus) {
            int statusCode = ((SyncAssignStatus) sourceEvent).getStatusCode();
            // only when status is not in process of new scan table period and the binlog split is
            // added back.
            // 1. 状态为INITIAL_ASSIGNING和NEWLY_ADDED_ASSIGNING_SNAPSHOT_FINISHED
            // 2. 即使已经完成动态加表，状态为NEWLY_ADDED_ASSIGNING_FINISHED， 也需要等到binlog被加回去
            if (statusCode == AssignerStatus.NEWLY_ADDED_ASSIGNING.getStatusCode()) {
                isCommitOffset = false;
                context.sendSourceEventToCoordinator(new SyncAssignStatusAck());
            } else if (statusCode == AssignerStatus.NEWLY_ADDED_ASSIGNING_FINISHED.getStatusCode()
                    && (suspendedStreamSplit == null || uncompletedStreamSplits.isEmpty())) {
                isCommitOffset = false;
            } else {
                isCommitOffset = true;
            }
        } else {
            super.handleSourceEvents(sourceEvent);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        if (!sourceConfig.isScanNewlyAddedTableEnabled() || isCommitOffset) {
            super.notifyCheckpointComplete(checkpointId);
        }
    }
}

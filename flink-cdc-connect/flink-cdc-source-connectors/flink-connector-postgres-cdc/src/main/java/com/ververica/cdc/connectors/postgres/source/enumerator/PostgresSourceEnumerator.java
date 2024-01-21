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

package com.ververica.cdc.connectors.postgres.source.enumerator;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.connectors.base.source.assigner.SplitAssigner;
import com.ververica.cdc.connectors.base.source.enumerator.IncrementalSourceEnumerator;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.postgres.source.PostgresDialect;
import com.ververica.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import com.ververica.cdc.connectors.postgres.source.events.SyncAssignStatus;
import com.ververica.cdc.connectors.postgres.source.events.SyncAssignStatusAck;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.PostgresReplicationConnection;
import io.debezium.connector.postgresql.spi.SlotState;

import static com.ververica.cdc.connectors.base.source.assigner.AssignerStatus.isInNewlyAddedProcess;

/**
 * The Postgres source enumerator that enumerates receive the split request and assign the split to
 * source readers.
 */
@Internal
public class PostgresSourceEnumerator extends IncrementalSourceEnumerator {

    private final PostgresDialect postgresDialect;
    private final PostgresSourceConfig sourceConfig;
    private volatile boolean isSuspendCommitOffsetAck = false;

    public PostgresSourceEnumerator(
            SplitEnumeratorContext<SourceSplitBase> context,
            PostgresSourceConfig sourceConfig,
            SplitAssigner splitAssigner,
            PostgresDialect postgresDialect,
            Boundedness boundedness) {
        this(context, sourceConfig, splitAssigner, postgresDialect, boundedness, -1);
    }

    public PostgresSourceEnumerator(
            SplitEnumeratorContext<SourceSplitBase> context,
            PostgresSourceConfig sourceConfig,
            SplitAssigner splitAssigner,
            PostgresDialect postgresDialect,
            Boundedness boundedness,
            int streamSplitTaskId) {
        super(context, sourceConfig, splitAssigner, boundedness, streamSplitTaskId);
        this.postgresDialect = postgresDialect;
        this.sourceConfig = sourceConfig;
    }

    @Override
    public void start() {
        createSlotForGlobalStreamSplit();
        super.start();
    }

    @Override
    protected void assignSplits() {
        // when enable scan newly added table, only isSuspendWallog = true will assign new snapshot
        // splits
        if (sourceConfig.isScanNewlyAddedTableEnabled()
                && streamSplitTaskId != -1
                && !isSuspendCommitOffsetAck
                && isInNewlyAddedProcess(splitAssigner.getAssignerStatus())) {
            // just return here, the reader has been put into readersAwaitingSplit, will be assigned
            // split again later
            return;
        }

        super.assignSplits();
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (sourceEvent instanceof SyncAssignStatusAck) {
            if (streamSplitTaskId != -1 && streamSplitTaskId == subtaskId) {
                this.isSuspendCommitOffsetAck = true;
            } else {
                throw new RuntimeException("Receive SyncAssignStatusAck from wrong subtask");
            }
        } else {
            super.handleSourceEvent(subtaskId, sourceEvent);
        }
    }

    @Override
    protected void syncWithReaders(int[] subtaskIds, Throwable t) {
        super.syncWithReaders(subtaskIds, t);
        // todo: 还需要文档润色一下
        // postgres enumerator will send its assign status to reader to tell whether to start commit
        // offset.
        if (sourceConfig.isScanNewlyAddedTableEnabled() && streamSplitTaskId != -1) {
            context.sendEventToSourceReader(
                    streamSplitTaskId,
                    new SyncAssignStatus(splitAssigner.getAssignerStatus().getStatusCode()));
        }
    }

    /**
     * Create slot for the unique global stream split.
     *
     * <p>Currently all startup modes need read the stream split. We need open the slot before
     * reading the globalStreamSplit to catch all data changes.
     */
    private void createSlotForGlobalStreamSplit() {
        try (PostgresConnection connection = postgresDialect.openJdbcConnection()) {
            SlotState slotInfo =
                    connection.getReplicationSlotState(
                            postgresDialect.getSlotName(), postgresDialect.getPluginName());
            // skip creating the replication slot when the slot exists.
            if (slotInfo != null) {
                return;
            }
            PostgresReplicationConnection replicationConnection =
                    postgresDialect.openPostgresReplicationConnection(connection);
            replicationConnection.createReplicationSlot();
            replicationConnection.close(false);

        } catch (Throwable t) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Fail to get or create slot for global stream split, the slot name is %s. Due to: ",
                            postgresDialect.getSlotName()),
                    t);
        }
    }
}

package com.ververica.cdc.connectors.base.source.reader.external;

import com.ververica.cdc.connectors.base.config.SourceConfig;
import com.ververica.cdc.connectors.base.dialect.DataSourceDialect;
import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.base.source.meta.split.SnapshotSplit;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.meta.split.StreamSplit;
import com.ververica.cdc.connectors.base.source.meta.wartermark.WatermarkKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

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

/** An abstract {@link FetchTask} implementation to read snapshot split. */
public abstract class AbstractScanFetcherTask implements FetchTask {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractScanFetcherTask.class);
    protected volatile boolean taskRunning = false;

    protected final SnapshotSplit snapshotSplit;

    public AbstractScanFetcherTask(SnapshotSplit snapshotSplit) {
        this.snapshotSplit = snapshotSplit;
    }

    @Override
    public void execute(Context context) throws Exception {
        LOG.info("Execute ScanFetchTask for split: {}", snapshotSplit);

        DataSourceDialect dialect = context.getDataSourceDialect();
        SourceConfig sourceConfig = context.getSourceConfig();

        taskRunning = true;

        final Offset lowWatermark = dialect.displayCurrentOffset(sourceConfig);
        LOG.info(
                "Snapshot step 1 - Determining low watermark {} for split {}",
                lowWatermark,
                snapshotSplit);
        dispathchLowWaterMarkEvent(context, snapshotSplit, lowWatermark);

        LOG.info("Snapshot step 2 - Snapshotting data");
        executeDataSnapshot(context);

        Offset highWatermark = dialect.displayCurrentOffset(sourceConfig);

        LOG.info(
                "Snapshot step 3 - Determining high watermark {} for split {}",
                highWatermark,
                snapshotSplit);
        dispathchHighWaterMarkEvent(context, snapshotSplit, highWatermark);

        // optimization that skip the stream read when the low watermark equals high watermark
        final StreamSplit backfillStreamSplit =
                createBackfillStreamSplit(lowWatermark, highWatermark);
        final boolean streamBackfillRequired =
                backfillStreamSplit
                        .getEndingOffset()
                        .isAfter(backfillStreamSplit.getStartingOffset());

        if (!streamBackfillRequired) {
            LOG.info(
                    "Skip the backfill {} for split {}: low watermark >= high watermark",
                    backfillStreamSplit,
                    snapshotSplit);
            dispathchEndWaterMarkEvent(
                    context, backfillStreamSplit, backfillStreamSplit.getEndingOffset());
        } else {
            executeBackfillTask(context, backfillStreamSplit);
        }

        taskRunning = false;
    }

    protected StreamSplit createBackfillStreamSplit(Offset lowWatermark, Offset highWatermark) {
        return new StreamSplit(
                snapshotSplit.splitId(),
                lowWatermark,
                highWatermark,
                new ArrayList<>(),
                snapshotSplit.getTableSchemas(),
                0);
    }

    protected abstract void executeBackfillTask(Context context, StreamSplit backfillStreamSplit)
            throws Exception;

    protected abstract void executeDataSnapshot(Context context) throws Exception;

    protected void dispathchLowWaterMarkEvent(
            Context context, SourceSplitBase split, Offset lowWatermark) throws Exception {
        if (context instanceof JdbcSourceFetchTaskContext) {
            ((JdbcSourceFetchTaskContext) context)
                    .getDispatcher()
                    .dispatchWatermarkEvent(
                            ((JdbcSourceFetchTaskContext) context)
                                    .getPartition()
                                    .getSourcePartition(),
                            split,
                            lowWatermark,
                            WatermarkKind.LOW);
            return;
        }
        throw new UnsupportedOperationException(
                "Unsupported Context type: " + context.getClass().toString());
    }

    protected void dispathchHighWaterMarkEvent(
            Context context, SourceSplitBase split, Offset highWatermark) throws Exception {
        if (context instanceof JdbcSourceFetchTaskContext) {
            ((JdbcSourceFetchTaskContext) context)
                    .getDispatcher()
                    .dispatchWatermarkEvent(
                            ((JdbcSourceFetchTaskContext) context)
                                    .getPartition()
                                    .getSourcePartition(),
                            split,
                            highWatermark,
                            WatermarkKind.HIGH);
            return;
        }
        throw new UnsupportedOperationException(
                "Unsupported Context type: " + context.getClass().toString());
    }

    protected void dispathchEndWaterMarkEvent(
            Context context, SourceSplitBase split, Offset endWatermark) throws Exception {
        if (context instanceof JdbcSourceFetchTaskContext) {
            ((JdbcSourceFetchTaskContext) context)
                    .getDispatcher()
                    .dispatchWatermarkEvent(
                            ((JdbcSourceFetchTaskContext) context)
                                    .getPartition()
                                    .getSourcePartition(),
                            split,
                            endWatermark,
                            WatermarkKind.END);
            return;
        }
        throw new UnsupportedOperationException(
                "Unsupported Context type: " + context.getClass().toString());
    }

    @Override
    public boolean isRunning() {
        return taskRunning;
    }

    @Override
    public SnapshotSplit getSplit() {
        return snapshotSplit;
    }

    @Override
    public void close() {
        taskRunning = false;
    }
}

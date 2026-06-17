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

import org.apache.fluss.client.table.scanner.MultiTableRecord;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.RowType;

import java.util.Collections;
import java.util.List;

/**
 * A wrapper that bundles a Fluss {@link ScanRecord} together with the table context (table path and
 * row type) needed for deserialization. This is the intermediate element type flowing from the
 * {@link FlussSplitReader} to the {@link FlussRecordEmitter}.
 *
 * <p>For snapshot records, {@code readRecordsCount} tracks how many records have been read so far
 * (used by the emitter to update {@code recordsToSkip} for checkpoint recovery). For log records,
 * {@code readRecordsCount} is always {@link #NO_READ_RECORDS_COUNT}.
 */
public class FlussSourceRecord {

    public static final long NO_READ_RECORDS_COUNT = -1;

    private final ScanRecord scanRecord;
    private final TablePath tablePath;
    private final RowType rowType;
    private final long readRecordsCount;
    private final List<String> primaryKeyNames;

    /** Creates a log record (no snapshot position tracking). */
    public FlussSourceRecord(MultiTableRecord multiTableRecord) {
        this(
                multiTableRecord.getScanRecord(),
                multiTableRecord.getTablePath(),
                multiTableRecord.getSchema().getRowType(),
                NO_READ_RECORDS_COUNT,
                multiTableRecord.getSchema().getPrimaryKeyColumnNames());
    }

    /** Creates a snapshot record with the cumulative read count for recovery. */
    public FlussSourceRecord(
            ScanRecord scanRecord,
            TablePath tablePath,
            RowType rowType,
            long readRecordsCount,
            List<String> primaryKeyNames) {
        this.scanRecord = scanRecord;
        this.tablePath = tablePath;
        this.rowType = rowType;
        this.readRecordsCount = readRecordsCount;
        this.primaryKeyNames =
                primaryKeyNames != null
                        ? Collections.unmodifiableList(primaryKeyNames)
                        : Collections.emptyList();
    }

    public ScanRecord getScanRecord() {
        return scanRecord;
    }

    public TablePath getTablePath() {
        return tablePath;
    }

    public RowType getRowType() {
        return rowType;
    }

    public long getReadRecordsCount() {
        return readRecordsCount;
    }

    /** Returns the primary key column names of the source table (empty for log tables). */
    public List<String> getPrimaryKeyNames() {
        return primaryKeyNames;
    }
}

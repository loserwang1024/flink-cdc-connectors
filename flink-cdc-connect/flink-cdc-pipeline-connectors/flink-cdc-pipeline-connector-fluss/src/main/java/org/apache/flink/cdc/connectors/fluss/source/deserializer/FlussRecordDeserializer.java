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

package org.apache.flink.cdc.connectors.fluss.source.deserializer;

import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.connectors.fluss.utils.FlussConversions;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;

import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.RowType;

import java.util.ArrayList;
import java.util.List;

/**
 * A CDC-specific implementation of {@link FlussDeserializer} that converts Fluss {@link
 * ScanRecord}s into Flink CDC {@link Event}s (DataChangeEvents).
 *
 * <p>This class maps Fluss ChangeType to the appropriate CDC operation type (INSERT, UPDATE,
 * DELETE).
 */
public class FlussRecordDeserializer implements FlussDeserializer<Event> {

    private static final long serialVersionUID = 1L;

    @Override
    public List<Event> deserialize(
            ScanRecord record, TablePath tablePath, RowType rowType, int schemaId) {
        List<Event> events = new ArrayList<>();
        TableId tableId = TableId.tableId(tablePath.getDatabaseName(), tablePath.getTableName());
        InternalRow row = record.getRow();
        ChangeType changeType = record.getChangeType();

        switch (changeType) {
            case APPEND_ONLY:
            case INSERT:
                {
                    RecordData after = convertFlussRowToCdcRecord(row, rowType);
                    events.add(DataChangeEvent.insertEvent(tableId, after));
                    break;
                }
            case UPDATE_BEFORE:
                // UPDATE_BEFORE is typically followed by UPDATE_AFTER.
                // We skip it here and handle the full update via UPDATE_AFTER.
                break;
            case UPDATE_AFTER:
                {
                    RecordData after = convertFlussRowToCdcRecord(row, rowType);
                    events.add(DataChangeEvent.replaceEvent(tableId, after));
                    break;
                }
            case DELETE:
                {
                    RecordData before = convertFlussRowToCdcRecord(row, rowType);
                    events.add(DataChangeEvent.deleteEvent(tableId, before));
                    break;
                }
            default:
                throw new IllegalArgumentException("Unsupported change type: " + changeType);
        }
        return events;
    }

    private RecordData convertFlussRowToCdcRecord(InternalRow row, RowType rowType) {
        int fieldCount = rowType.getFieldCount();
        org.apache.flink.cdc.common.types.RowType cdcRowType =
                (org.apache.flink.cdc.common.types.RowType) FlussConversions.toCdcType(rowType);
        // TODO: 1. cache this generator later to improve.
        // TODO: 当前的实现不优雅，且不支持非负责类型，建议通过DataTypeVisitor来优雅实现.
        BinaryRecordDataGenerator generator = new BinaryRecordDataGenerator(cdcRowType);
        Object[] rowFields = new Object[fieldCount];
        List<DataField> fields = rowType.getFields();
        for (int i = 0; i < fieldCount; i++) {
            if (row.isNullAt(i)) {
                rowFields[i] = null;
            } else {
                rowFields[i] = convertFlussField(row, i, fields.get(i).getType());
            }
        }
        return generator.generate(rowFields);
    }

    private Object convertFlussField(
            InternalRow row, int pos, org.apache.fluss.types.DataType flussType) {
        if (flussType instanceof org.apache.fluss.types.BooleanType) {
            return row.getBoolean(pos);
        } else if (flussType instanceof org.apache.fluss.types.TinyIntType) {
            return row.getByte(pos);
        } else if (flussType instanceof org.apache.fluss.types.SmallIntType) {
            return row.getShort(pos);
        } else if (flussType instanceof org.apache.fluss.types.IntType) {
            return row.getInt(pos);
        } else if (flussType instanceof org.apache.fluss.types.BigIntType) {
            return row.getLong(pos);
        } else if (flussType instanceof org.apache.fluss.types.FloatType) {
            return row.getFloat(pos);
        } else if (flussType instanceof org.apache.fluss.types.DoubleType) {
            return row.getDouble(pos);
        } else if (flussType instanceof org.apache.fluss.types.CharType) {
            int length = ((org.apache.fluss.types.CharType) flussType).getLength();
            return BinaryStringData.fromString(row.getChar(pos, length).toString());
        } else if (flussType instanceof org.apache.fluss.types.StringType) {
            return BinaryStringData.fromString(row.getString(pos).toString());
        } else if (flussType instanceof org.apache.fluss.types.DecimalType) {
            org.apache.fluss.types.DecimalType decimalType =
                    (org.apache.fluss.types.DecimalType) flussType;
            org.apache.fluss.row.Decimal flussDecimal =
                    row.getDecimal(pos, decimalType.getPrecision(), decimalType.getScale());
            return DecimalData.fromBigDecimal(
                    flussDecimal.toBigDecimal(),
                    decimalType.getPrecision(),
                    decimalType.getScale());
        } else if (flussType instanceof org.apache.fluss.types.DateType) {
            return row.getInt(pos);
        } else if (flussType instanceof org.apache.fluss.types.TimeType) {
            return row.getInt(pos);
        } else if (flussType instanceof org.apache.fluss.types.TimestampType) {
            int precision = ((org.apache.fluss.types.TimestampType) flussType).getPrecision();
            org.apache.fluss.row.TimestampNtz flussTimestamp = row.getTimestampNtz(pos, precision);
            return TimestampData.fromMillis(
                    flussTimestamp.getMillisecond(), flussTimestamp.getNanoOfMillisecond());
        } else if (flussType instanceof org.apache.fluss.types.LocalZonedTimestampType) {
            int precision =
                    ((org.apache.fluss.types.LocalZonedTimestampType) flussType).getPrecision();
            org.apache.fluss.row.TimestampLtz flussTimestamp = row.getTimestampLtz(pos, precision);
            return LocalZonedTimestampData.fromEpochMillis(
                    flussTimestamp.getEpochMillisecond(), flussTimestamp.getNanoOfMillisecond());
        } else if (flussType instanceof org.apache.fluss.types.BinaryType) {
            int length = ((org.apache.fluss.types.BinaryType) flussType).getLength();
            return row.getBinary(pos, length);
        } else if (flussType instanceof org.apache.fluss.types.BytesType) {
            return row.getBytes(pos);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported Fluss data type for deserialization: "
                            + flussType.getClass().getSimpleName());
        }
    }
}

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

package org.apache.flink.cdc.connectors.fluss.source.split;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Serializer for {@link FlussSplitBase} and its subclasses. Uses a type tag byte to distinguish
 * between split types: log (0), snapshot (1), and hybrid snapshot-log (2).
 */
public class FlussSplitSerializer implements SimpleVersionedSerializer<FlussSplitBase> {

    private static final int VERSION = 1;

    private static final byte TYPE_LOG = 0;
    private static final byte TYPE_SNAPSHOT = 1;
    private static final byte TYPE_HYBRID = 2;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(FlussSplitBase split) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(baos)) {

            // Write type tag
            if (split.isHybridSnapshotLogSplit()) {
                out.writeByte(TYPE_HYBRID);
            } else if (split.isSnapshotSplit()) {
                out.writeByte(TYPE_SNAPSHOT);
            } else {
                out.writeByte(TYPE_LOG);
            }

            // Write common fields
            out.writeUTF(split.getPhysicalTablePath().getDatabaseName());
            out.writeUTF(split.getPhysicalTablePath().getTableName());
            out.writeLong(split.getTableBucket().getTableId());
            out.writeInt(split.getTableBucket().getBucket());

            Long partitionId = split.getTableBucket().getPartitionId();
            out.writeBoolean(partitionId != null);
            if (partitionId != null) {
                out.writeLong(partitionId);
            }
            String partitionName = split.getPhysicalTablePath().getPartitionName();
            out.writeBoolean(partitionName != null);
            if (partitionName != null) {
                out.writeUTF(partitionName);
            }

            // Write type-specific fields
            if (split.isHybridSnapshotLogSplit()) {
                FlussHybridSnapshotLogSplit hybrid = split.asHybridSnapshotLogSplit();
                out.writeLong(hybrid.getSnapshotId());
                out.writeLong(hybrid.getRecordsToSkip());
                out.writeLong(hybrid.getLogStartingOffset());
                out.writeBoolean(hybrid.isSnapshotFinished());
            } else if (split.isSnapshotSplit()) {
                FlussSnapshotSplit snapshot = split.asSnapshotSplit();
                out.writeLong(snapshot.getSnapshotId());
                out.writeLong(snapshot.getRecordsToSkip());
            } else {
                FlussLogSplit log = split.asLogSplit();
                out.writeLong(log.getStartingOffset());
            }
            return baos.toByteArray();
        }
    }

    @Override
    public FlussSplitBase deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputViewStreamWrapper in = new DataInputViewStreamWrapper(bais)) {

            byte type = in.readByte();

            // Read common fields
            String databaseName = in.readUTF();
            String tableName = in.readUTF();
            long tableId = in.readLong();
            int bucket = in.readInt();

            boolean hasPartitionId = in.readBoolean();
            Long partitionId = hasPartitionId ? in.readLong() : null;

            boolean hasPartitionName = in.readBoolean();
            String partitionName = hasPartitionName ? in.readUTF() : null;

            TablePath tablePath = new TablePath(databaseName, tableName);
            PhysicalTablePath physicalTablePath =
                    partitionName != null
                            ? PhysicalTablePath.of(tablePath, partitionName)
                            : PhysicalTablePath.of(tablePath);
            TableBucket tableBucket =
                    partitionId != null
                            ? new TableBucket(tableId, partitionId, bucket)
                            : new TableBucket(tableId, bucket);

            // Read type-specific fields
            switch (type) {
                case TYPE_HYBRID:
                    {
                        long snapshotId = in.readLong();
                        long recordsToSkip = in.readLong();
                        long logStartingOffset = in.readLong();
                        boolean snapshotFinished = in.readBoolean();
                        return new FlussHybridSnapshotLogSplit(
                                physicalTablePath,
                                tableBucket,
                                snapshotId,
                                recordsToSkip,
                                logStartingOffset,
                                snapshotFinished);
                    }
                case TYPE_SNAPSHOT:
                    {
                        long snapshotId = in.readLong();
                        long recordsToSkip = in.readLong();
                        return new FlussSnapshotSplit(
                                physicalTablePath, tableBucket, snapshotId, recordsToSkip);
                    }
                case TYPE_LOG:
                default:
                    {
                        long startingOffset = in.readLong();
                        return new FlussLogSplit(physicalTablePath, tableBucket, startingOffset);
                    }
            }
        }
    }
}

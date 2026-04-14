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

import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.RowType;

import java.io.Serializable;
import java.util.List;

/**
 * A generic deserializer interface for converting Fluss {@link ScanRecord}s into output records of
 * type {@code T}. This interface is intentionally not bound to any specific CDC type, allowing the
 * Fluss source to be reused with different output types.
 *
 * @param <T> The type of output records produced by this deserializer.
 */
public interface FlussDeserializer<T> extends Serializable {

    /**
     * Deserializes a Fluss {@link ScanRecord} into a list of output records.
     *
     * @param record The Fluss scan record to deserialize.
     * @param tablePath The Fluss table path (database.table).
     * @param rowType The Fluss row type of the table schema.
     * @return A list of deserialized output records.
     */
    List<T> deserialize(ScanRecord record, TablePath tablePath, RowType rowType);
}

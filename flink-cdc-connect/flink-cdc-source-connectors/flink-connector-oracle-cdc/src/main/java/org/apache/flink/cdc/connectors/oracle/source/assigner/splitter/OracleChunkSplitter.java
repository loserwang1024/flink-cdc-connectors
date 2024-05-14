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

package org.apache.flink.cdc.connectors.oracle.source.assigner.splitter;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.source.assigner.splitter.JdbcSourceChunkSplitter;
import org.apache.flink.cdc.connectors.base.utils.ObjectUtils;
import org.apache.flink.cdc.connectors.oracle.source.utils.OracleTypeUtils;
import org.apache.flink.cdc.connectors.oracle.source.utils.OracleUtils;
import org.apache.flink.cdc.connectors.oracle.util.ChunkUtils;
import org.apache.flink.table.types.DataType;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import oracle.sql.ROWID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.sql.SQLException;

/**
 * The {@code ChunkSplitter} used to split Oracle table into a set of chunks for JDBC data source.
 */
@Internal
public class OracleChunkSplitter extends JdbcSourceChunkSplitter {

    private static final Logger LOG = LoggerFactory.getLogger(OracleChunkSplitter.class);

    public OracleChunkSplitter(JdbcSourceConfig sourceConfig, JdbcDataSourceDialect dialect) {
        super(sourceConfig, dialect);
    }

    @Override
    public Object queryNextChunkMax(
            JdbcConnection jdbc,
            TableId tableId,
            String columnName,
            int chunkSize,
            Object includedLowerBound)
            throws SQLException {
        return OracleUtils.queryNextChunkMax(
                jdbc, tableId, columnName, chunkSize, includedLowerBound);
    }

    @Override
    protected Long queryApproximateRowCnt(JdbcConnection jdbc, TableId tableId)
            throws SQLException {
        return OracleUtils.queryApproximateRowCnt(jdbc, tableId);
    }

    @Override
    public DataType fromDbzColumn(Column splitColumn) {
        return OracleTypeUtils.fromDbzColumn(splitColumn);
    }

    @Override
    protected boolean isEvenlySplitColumn(Column splitColumn) {
        // use ROWID get splitUnevenlySizedChunks by default
        if (splitColumn.name().equals(ROWID.class.getSimpleName())) {
            return false;
        }

        return super.isEvenlySplitColumn(splitColumn);
    }

    /** ChunkEnd less than or equal to max. */
    @Override
    protected boolean isChunkEndLeMax(Object chunkEnd, Object max) {
        boolean chunkEndMaxCompare;
        if (chunkEnd instanceof ROWID && max instanceof ROWID) {
            chunkEndMaxCompare =
                    ROWID.compareBytes(((ROWID) chunkEnd).getBytes(), ((ROWID) max).getBytes())
                            <= 0;
        } else {
            chunkEndMaxCompare = chunkEnd != null && ObjectUtils.compare(chunkEnd, max) <= 0;
        }
        return chunkEndMaxCompare;
    }

    /** ChunkEnd greater than or equal to max. */
    @Override
    protected boolean isChunkEndGeMax(Object chunkEnd, Object max) {
        boolean chunkEndMaxCompare;
        if (chunkEnd instanceof ROWID && max instanceof ROWID) {
            chunkEndMaxCompare =
                    ROWID.compareBytes(((ROWID) chunkEnd).getBytes(), ((ROWID) max).getBytes())
                            >= 0;
        } else {
            chunkEndMaxCompare = chunkEnd != null && ObjectUtils.compare(chunkEnd, max) >= 0;
        }
        return chunkEndMaxCompare;
    }

    @Override
    protected Column getSplitColumn(Table table, @Nullable String chunkKeyColumn) {
        // Use the ROWID column as the chunk key column by default for oracle cdc connector
        return ChunkUtils.getChunkKeyColumn(table, chunkKeyColumn);
    }
}

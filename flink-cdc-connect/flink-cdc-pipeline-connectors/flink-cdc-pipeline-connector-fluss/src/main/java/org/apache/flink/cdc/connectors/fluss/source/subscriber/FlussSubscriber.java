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

package org.apache.flink.cdc.connectors.fluss.source.subscriber;

import org.apache.fluss.client.Connection;
import org.apache.fluss.metadata.TablePath;

import java.io.Serializable;
import java.util.Set;

/**
 * Pluggable abstraction for subscribing to a set of Fluss tables. Implementations determine which
 * tables the source should read. Two built-in implementations are provided:
 *
 * <ul>
 *   <li>{@link PatternSubscriber} - matches fully-qualified table names ("database.tableName") with
 *       a user-defined regex.
 *   <li>{@link FlussTableSubscriber} - reads the subscription list from a Fluss primary-key table.
 * </ul>
 */
public interface FlussSubscriber extends Serializable {

    /**
     * Returns the set of Fluss table paths to subscribe to.
     *
     * @param connection The Fluss client connection used to discover tables. Implementations may
     *     obtain the {@link org.apache.fluss.client.admin.Admin} via {@code connection.getAdmin()},
     *     or open a {@link org.apache.fluss.client.table.Table} to read metadata from another Fluss
     *     table. Implementations must NOT close the connection.
     * @return A set of {@link TablePath} representing the tables to read.
     * @throws Exception if the discovery fails.
     */
    Set<TablePath> getSubscribedTablePaths(Connection connection) throws Exception;
}

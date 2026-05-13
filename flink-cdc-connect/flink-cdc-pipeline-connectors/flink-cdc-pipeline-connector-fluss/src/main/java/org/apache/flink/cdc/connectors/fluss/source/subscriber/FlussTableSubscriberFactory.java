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

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;
import org.apache.flink.cdc.common.configuration.Configuration;

import java.util.HashSet;
import java.util.Set;

/**
 * {@link FlussSubscriberFactory} for {@link FlussTableSubscriber}. Activated when {@code
 * subscriber.type = 'fluss'}.
 *
 * <p>This factory consumes a sub-configuration whose keys have been stripped of the {@code
 * "subscriber."} prefix by {@link
 * org.apache.flink.cdc.connectors.fluss.factory.FlussDataSourceFactory}. The user-facing options
 * are {@code subscriber.fluss = '<db.table>'} and {@code subscriber.fluss.limit = <n>}; this
 * factory only sees the suffixes {@code fluss} and {@code fluss.limit}.
 */
public class FlussTableSubscriberFactory implements FlussSubscriberFactory {

    private static final long serialVersionUID = 1L;

    public static final String IDENTIFIER = "fluss";

    /**
     * The fully-qualified subscription table path. The user-facing key is {@code "subscriber." +
     * key()} == {@code "subscriber.fluss"}.
     */
    public static final ConfigOption<String> FLUSS =
            ConfigOptions.key("fluss")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The fully-qualified name ('database.tableName') of a Fluss primary-key "
                                    + "table whose rows describe which Fluss tables to subscribe. "
                                    + "The first column of each row must contain the fully-qualified "
                                    + "name of a target table. Required when subscriber.type is 'fluss'.");

    /** Fluss订阅表的上限数量. TODO: 是否需要这个参数待讨论. subscriber */
    public static final ConfigOption<Integer> SUBSCRIBER_TABLE_LIMIT =
            ConfigOptions.key("subscriber_table_limit")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("");

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public FlussSubscriber create(Configuration subscriberConfig) {
        String subscriptionTable = subscriberConfig.get(FLUSS);
        if (subscriptionTable == null || subscriptionTable.isEmpty()) {
            throw new IllegalArgumentException(
                    "'subscriber."
                            + FLUSS.key()
                            + "' is required when 'subscriber.type' is '"
                            + IDENTIFIER
                            + "'.");
        }
        int limit = subscriberConfig.get(SUBSCRIBER_TABLE_LIMIT);
        return new FlussTableSubscriber(subscriptionTable, limit);
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FLUSS);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SUBSCRIBER_TABLE_LIMIT);
        return options;
    }
}

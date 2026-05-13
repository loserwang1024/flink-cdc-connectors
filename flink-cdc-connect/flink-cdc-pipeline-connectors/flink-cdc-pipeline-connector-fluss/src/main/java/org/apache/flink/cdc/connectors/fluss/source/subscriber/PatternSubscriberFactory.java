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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * {@link FlussSubscriberFactory} for {@link PatternSubscriber}. Activated when {@code
 * subscriber.type = 'pattern'}.
 *
 * <p>This factory consumes a sub-configuration whose keys have been stripped of the {@code
 * "subscriber."} prefix by {@link
 * org.apache.flink.cdc.connectors.fluss.factory.FlussDataSourceFactory}. From the user's point of
 * view the option is set as {@code subscriber.pattern = '<regex>'}; this factory only sees the
 * suffix {@code pattern}.
 */
public class PatternSubscriberFactory implements FlussSubscriberFactory {

    private static final long serialVersionUID = 1L;

    public static final String IDENTIFIER = "pattern";

    /**
     * The single regex parameter of this factory. The user-facing fully-qualified key is {@code
     * "subscriber." + key()} == {@code "subscriber.pattern"}.
     */
    public static final ConfigOption<String> PATTERN =
            ConfigOptions.key("pattern")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "A Java regular expression matched against the fully-qualified "
                                    + "'database.tableName' of every visible Fluss table. "
                                    + "Required when subscriber.type is 'pattern'. "
                                    + "Example: 'source_db\\..*|audit_db\\.events_.*'.");

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public FlussSubscriber create(Configuration subscriberConfig) {
        String pattern = subscriberConfig.get(PATTERN);
        if (pattern == null || pattern.isEmpty()) {
            throw new IllegalArgumentException(
                    "'subscriber."
                            + PATTERN.key()
                            + "' is required when 'subscriber.type' is '"
                            + IDENTIFIER
                            + "'.");
        }
        return new PatternSubscriber(pattern);
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PATTERN);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }
}

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

package org.apache.flink.cdc.connectors.fluss.factory;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.factories.DataSourceFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.connectors.fluss.source.FlussDataSource;
import org.apache.flink.cdc.connectors.fluss.source.subscriber.FlussSubscriber;
import org.apache.flink.cdc.connectors.fluss.source.subscriber.FlussSubscriberFactory;

import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;

import java.util.HashSet;
import java.util.ServiceLoader;
import java.util.Set;

import static org.apache.flink.cdc.connectors.fluss.source.FlussDataSourceOptions.BOOTSTRAP_SERVERS;
import static org.apache.flink.cdc.connectors.fluss.source.FlussDataSourceOptions.CLIENT_PROPERTIES_PREFIX;
import static org.apache.flink.cdc.connectors.fluss.source.FlussDataSourceOptions.SCAN_DISCOVERY_INTERVAL;
import static org.apache.flink.cdc.connectors.fluss.source.FlussDataSourceOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.cdc.connectors.fluss.source.FlussDataSourceOptions.SCAN_STARTUP_TIMESTAMP;
import static org.apache.flink.cdc.connectors.fluss.source.FlussDataSourceOptions.SUBSCRIBER_OPTIONS_PREFIX;
import static org.apache.flink.cdc.connectors.fluss.source.FlussDataSourceOptions.SUBSCRIBER_TYPE;

/** Factory for creating configured instances of {@link FlussDataSource}. */
public class FlussDataSourceFactory implements DataSourceFactory {

    public static final String IDENTIFIER = "fluss";

    @Override
    public DataSource createDataSource(Context context) {
        FactoryHelper.createFactoryHelper(this, context)
                .validateExcept(CLIENT_PROPERTIES_PREFIX, SUBSCRIBER_OPTIONS_PREFIX);

        org.apache.flink.cdc.common.configuration.Configuration factoryConfiguration =
                context.getFactoryConfiguration();

        String startupMode = factoryConfiguration.get(SCAN_STARTUP_MODE);

        Configuration flussConfig = toFlussClientConfig(factoryConfiguration);

        FlussSubscriber subscriber =
                createSubscriber(factoryConfiguration, context.getClassLoader());

        OffsetsInitializer offsetsInitializer =
                getOffsetsInitializer(startupMode, factoryConfiguration);

        long scanDiscoveryIntervalMs = factoryConfiguration.get(SCAN_DISCOVERY_INTERVAL).toMillis();

        return new FlussDataSource(
                flussConfig, subscriber, offsetsInitializer, scanDiscoveryIntervalMs);
    }

    /**
     * Discovers a {@link FlussSubscriberFactory} whose {@link FlussSubscriberFactory#identifier()}
     * matches the value of {@code subscriber.type}, and delegates subscriber creation to it.
     */
    private static FlussSubscriber createSubscriber(
            org.apache.flink.cdc.common.configuration.Configuration config,
            ClassLoader classLoader) {
        String type = config.get(SUBSCRIBER_TYPE);
        ClassLoader loader =
                classLoader != null ? classLoader : Thread.currentThread().getContextClassLoader();
        ServiceLoader<FlussSubscriberFactory> serviceLoader =
                ServiceLoader.load(FlussSubscriberFactory.class, loader);

        FlussSubscriberFactory matched = null;
        Set<String> known = new HashSet<>();
        for (FlussSubscriberFactory factory : serviceLoader) {
            known.add(factory.identifier());
            if (factory.identifier().equalsIgnoreCase(type)) {
                if (matched != null) {
                    throw new IllegalStateException(
                            "Multiple FlussSubscriberFactory implementations found for identifier '"
                                    + type
                                    + "': "
                                    + matched.getClass().getName()
                                    + " and "
                                    + factory.getClass().getName());
                }
                matched = factory;
            }
        }
        if (matched == null) {
            throw new IllegalArgumentException(
                    "Unsupported '"
                            + SUBSCRIBER_TYPE.key()
                            + "' value: '"
                            + type
                            + "'. Available subscriber types: "
                            + known
                            + ".");
        }
        // Extract the 'subscriber.' sub-configuration so each factory only sees its own keys with
        // the prefix stripped (e.g. 'subscriber.fluss.limit' -> 'fluss.limit').
        return matched.create(extractSubConfig(config, SUBSCRIBER_OPTIONS_PREFIX));
    }

    /**
     * Returns a new {@link org.apache.flink.cdc.common.configuration.Configuration} containing only
     * the entries of {@code source} whose keys start with {@code prefix}, with {@code prefix}
     * stripped from each key.
     */
    private static org.apache.flink.cdc.common.configuration.Configuration extractSubConfig(
            org.apache.flink.cdc.common.configuration.Configuration source, String prefix) {
        java.util.Map<String, String> sub = new java.util.HashMap<>();
        for (java.util.Map.Entry<String, String> entry : source.toMap().entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(prefix)) {
                sub.put(key.substring(prefix.length()), entry.getValue());
            }
        }
        return org.apache.flink.cdc.common.configuration.Configuration.fromMap(sub);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(BOOTSTRAP_SERVERS);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SUBSCRIBER_TYPE);
        options.add(SCAN_STARTUP_MODE);
        options.add(SCAN_STARTUP_TIMESTAMP);
        options.add(SCAN_DISCOVERY_INTERVAL);
        return options;
    }

    private static OffsetsInitializer getOffsetsInitializer(
            String startupMode, org.apache.flink.cdc.common.configuration.Configuration config) {
        if ("earliest".equalsIgnoreCase(startupMode)) {
            return OffsetsInitializer.earliest();
        } else if ("latest".equalsIgnoreCase(startupMode)) {
            return OffsetsInitializer.latest();
        } else if ("full".equalsIgnoreCase(startupMode)) {
            return OffsetsInitializer.full();
        } else if ("timestamp".equalsIgnoreCase(startupMode)) {
            String timestampStr = config.get(SCAN_STARTUP_TIMESTAMP);
            if (timestampStr == null || timestampStr.isEmpty()) {
                throw new IllegalArgumentException(
                        "'scan.startup.timestamp' is required when scan.startup.mode is 'timestamp'.");
            }
            long timestampMs = parseTimestamp(timestampStr);
            return OffsetsInitializer.timestamp(timestampMs);
        } else {
            throw new IllegalArgumentException("Unsupported startup mode: " + startupMode);
        }
    }

    /**
     * Parses a timestamp string to a long value. Supports both epoch milliseconds and 'yyyy-MM-dd
     * HH:mm:ss' format.
     */
    private static long parseTimestamp(String timestampStr) {
        if (timestampStr.matches("\\d+")) {
            return Long.parseLong(timestampStr);
        }
        try {
            return java.time.LocalDateTime.parse(
                            timestampStr,
                            java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                    .atZone(java.time.ZoneId.systemDefault())
                    .toInstant()
                    .toEpochMilli();
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid 'scan.startup.timestamp' value '%s'. "
                                    + "Expected format: 'yyyy-MM-dd HH:mm:ss' or epoch milliseconds.",
                            timestampStr),
                    e);
        }
    }

    private static Configuration toFlussClientConfig(
            org.apache.flink.cdc.common.configuration.Configuration factoryConfig) {
        Configuration flussConfig = new Configuration();
        flussConfig.setString(
                ConfigOptions.BOOTSTRAP_SERVERS.key(), factoryConfig.get(BOOTSTRAP_SERVERS));

        factoryConfig
                .toMap()
                .forEach(
                        (key, value) -> {
                            if (key.startsWith(CLIENT_PROPERTIES_PREFIX)) {
                                flussConfig.setString(key.substring("properties.".length()), value);
                            }
                        });
        return flussConfig;
    }
}

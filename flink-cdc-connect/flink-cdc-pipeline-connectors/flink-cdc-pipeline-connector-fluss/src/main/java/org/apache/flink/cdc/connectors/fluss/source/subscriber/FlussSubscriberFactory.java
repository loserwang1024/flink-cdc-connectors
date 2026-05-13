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
import org.apache.flink.cdc.common.configuration.Configuration;

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;

/**
 * SPI factory that creates a {@link FlussSubscriber}. Implementations are discovered at runtime via
 * Java's {@link java.util.ServiceLoader} mechanism. To register a new subscriber type, add a line
 * with the fully-qualified factory class name to:
 *
 * <pre>{@code
 * META-INF/services/org.apache.flink.cdc.connectors.fluss.source.subscriber.FlussSubscriberFactory
 * }</pre>
 *
 * <p>The subscriber type is selected by the user via the {@code subscriber.type} option, which is
 * matched (case-insensitively) against {@link #identifier()}. The factory then receives a
 * <em>sub-configuration</em> whose keys are stripped of the {@code "subscriber."} prefix; for
 * example a user-set {@code subscriber.fluss.limit = 100} is visible to the factory as {@code
 * fluss.limit = 100}. Each factory should therefore declare its options without the {@code
 * "subscriber."} prefix.
 */
public interface FlussSubscriberFactory extends Serializable {

    /**
     * The unique identifier of this subscriber factory, used to match against the {@code
     * subscriber.type} option. Must be lowercase, e.g. {@code "pattern"} or {@code "fluss"}.
     */
    String identifier();

    /**
     * Creates a {@link FlussSubscriber} from the given <em>sub-configuration</em>. The keys in
     * {@code subscriberConfig} have already been stripped of the {@code "subscriber."} prefix.
     */
    FlussSubscriber create(Configuration subscriberConfig);

    /** Options this factory requires in addition to {@link #optionalOptions()}. */
    default Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    /** Options this factory consumes in addition to {@link #requiredOptions()}. */
    default Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }
}

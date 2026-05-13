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
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.metadata.TablePath;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * A {@link FlussSubscriber} that subscribes to Fluss tables whose fully-qualified name (formatted
 * as {@code "database.tableName"}) matches a user-provided Java regular expression.
 *
 * <p>The pattern is matched against the fully-qualified name of every table across every database
 * visible to the Fluss cluster. A single regex can therefore span multiple databases, e.g. {@code
 * "source_db\\..*|audit_db\\.events_.*"}.
 *
 * <p>Note: dots in database or table names must be escaped (e.g. {@code "db\\.table"}) because
 * {@code .} is a regex meta-character.
 */
public class PatternSubscriber implements FlussSubscriber {

    private static final long serialVersionUID = 1L;

    /** Java regex matching fully-qualified {@code database.tableName} strings. */
    private final String pattern;

    public PatternSubscriber(String pattern) {
        this.pattern = pattern;
    }

    @Override
    public Set<TablePath> getSubscribedTablePaths(Connection connection) throws Exception {
        Pattern compiled = Pattern.compile("^" + pattern + "$");

        Admin admin = connection.getAdmin();
        Set<TablePath> matched = new LinkedHashSet<>();
        List<String> databases = admin.listDatabases().get();
        for (String database : databases) {
            List<String> tables = admin.listTables(database).get();
            for (String tableName : tables) {
                String fqn = database + "." + tableName;
                if (compiled.matcher(fqn).matches()) {
                    matched.add(new TablePath(database, tableName));
                }
            }
        }
        return matched;
    }

    public String getPattern() {
        return pattern;
    }
}

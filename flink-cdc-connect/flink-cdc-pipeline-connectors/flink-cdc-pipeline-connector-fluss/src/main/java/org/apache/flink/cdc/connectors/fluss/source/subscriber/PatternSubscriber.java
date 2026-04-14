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

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.metadata.TablePath;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * A {@link FlussSubscriber} that subscribes to tables matching a wildcard pattern within a given
 * database. The pattern uses '*' as a wildcard, which is converted to regex '.*' for matching.
 */
public class PatternSubscriber implements FlussSubscriber {

    private static final long serialVersionUID = 1L;

    private final String database;
    private final String tablePattern;

    public PatternSubscriber(String database, String tablePattern) {
        this.database = database;
        this.tablePattern = tablePattern;
    }

    @Override
    public Set<TablePath> getSubscribedTablePaths(Admin admin) throws Exception {
        String regex = "^" + tablePattern.replace("*", ".*") + "$";
        Pattern pattern = Pattern.compile(regex);

        List<String> allTables = admin.listTables(database).get();
        Set<TablePath> matched = new LinkedHashSet<>();
        for (String tableName : allTables) {
            if (pattern.matcher(tableName).matches()) {
                matched.add(new TablePath(database, tableName));
            }
        }
        return matched;
    }

    public String getDatabase() {
        return database;
    }

    public String getTablePattern() {
        return tablePattern;
    }
}

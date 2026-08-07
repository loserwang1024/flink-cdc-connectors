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

package org.apache.flink.cdc.connectors.fluss.sink.validator;

import org.apache.flink.table.api.ValidationException;

import org.apache.fluss.metadata.Schema;
import org.apache.fluss.types.DataType;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Utilities for Fluss schema validation. */
class SchemaValidationUtils {

    static void validateTargetContainsAllInputColumns(Schema inputSchema, Schema targetSchema) {
        List<String> inputColumnNames = inputSchema.getColumnNames();
        Set<String> targetColumnNames = new HashSet<>(targetSchema.getColumnNames());
        List<String> missingColumns =
                inputColumnNames.stream()
                        .filter(columnName -> !targetColumnNames.contains(columnName))
                        .collect(Collectors.toList());
        if (!missingColumns.isEmpty()) {
            throw new ValidationException(
                    "The target Fluss table schema is not a superset of the input schema in "
                            + "schema-validation-mode 'target-superset'. Missing input columns in "
                            + "target Fluss table: "
                            + missingColumns
                            + ". Input columns: "
                            + inputColumnNames
                            + ". Target columns: "
                            + targetSchema.getColumnNames()
                            + ".");
        }
    }

    static Map<Integer, Integer> generateIndexMappingAndValidateMappedFieldTypes(
            Schema inputSchema, Schema targetSchema) {
        Map<String, Integer> inputColumnIndex = new HashMap<>();
        List<String> inputColumnNames = inputSchema.getColumnNames();
        for (int inputIndex = 0; inputIndex < inputColumnNames.size(); inputIndex++) {
            inputColumnIndex.put(inputColumnNames.get(inputIndex), inputIndex);
        }

        List<String> targetColumnNames = targetSchema.getColumnNames();
        Map<Integer, Integer> indexMapping = new HashMap<>();
        for (int targetIndex = 0; targetIndex < targetColumnNames.size(); targetIndex++) {
            String columnName = targetColumnNames.get(targetIndex);
            Integer inputIndex = inputColumnIndex.get(columnName);
            if (inputIndex != null) {
                indexMapping.put(targetIndex, inputIndex);
                validateMappedFieldType(
                        columnName,
                        inputSchema.getRowType().getTypeAt(inputIndex),
                        targetSchema.getRowType().getTypeAt(targetIndex));
            }
        }
        return indexMapping;
    }

    private static void validateMappedFieldType(
            String columnName, DataType inputDataType, DataType targetDataType) {
        if (!inputDataType.copy(false).equals(targetDataType.copy(false))) {
            throw new ValidationException(
                    "The data type of column "
                            + columnName
                            + " is changed from "
                            + inputDataType
                            + " to "
                            + targetDataType);
        }
    }

    private SchemaValidationUtils() {}
}

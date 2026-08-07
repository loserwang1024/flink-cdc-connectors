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
import org.apache.fluss.types.DataTypes;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SchemaValidator}. */
class SchemaValidatorTest {

    @Test
    void testPermissiveAllowsInputOnlyColumns() {
        SchemaValidator validator = new PermissiveSchemaValidator();

        Map<Integer, Integer> indexMapping =
                validator.validateAndGenerateIndexMapping(
                        schema(
                                "id", DataTypes.INT(),
                                "name", DataTypes.STRING(),
                                "age", DataTypes.INT()),
                        schema(
                                "id", DataTypes.INT(),
                                "name", DataTypes.STRING()));

        assertThat(indexMapping).containsExactlyInAnyOrderEntriesOf(Map.of(0, 0, 1, 1));
    }

    @Test
    void testPermissivePadsTargetOnlyColumns() {
        SchemaValidator validator = new PermissiveSchemaValidator();

        Map<Integer, Integer> indexMapping =
                validator.validateAndGenerateIndexMapping(
                        schema("id", DataTypes.INT()),
                        schema(
                                "id", DataTypes.INT(),
                                "name", DataTypes.STRING()));

        assertThat(indexMapping).containsExactlyInAnyOrderEntriesOf(Map.of(0, 0));
    }

    @Test
    void testTargetSupersetAllowsTargetOnlyColumnsAndReorderedColumns() {
        SchemaValidator validator = new TargetSupersetSchemaValidator();

        Map<Integer, Integer> indexMapping =
                validator.validateAndGenerateIndexMapping(
                        schema(
                                "id", DataTypes.INT(),
                                "name", DataTypes.STRING()),
                        schema(
                                "name", DataTypes.STRING(),
                                "age", DataTypes.INT(),
                                "id", DataTypes.INT()));

        assertThat(indexMapping).containsExactlyInAnyOrderEntriesOf(Map.of(0, 1, 2, 0));
    }

    @Test
    void testTargetSupersetRejectsInputOnlyColumns() {
        SchemaValidator validator = new TargetSupersetSchemaValidator();

        assertThatThrownBy(
                        () ->
                                validator.validateSchema(
                                        schema(
                                                "id", DataTypes.INT(),
                                                "name", DataTypes.STRING(),
                                                "age", DataTypes.INT()),
                                        schema(
                                                "id", DataTypes.INT(),
                                                "name", DataTypes.STRING())))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("not a superset")
                .hasMessageContaining("age");
    }

    @Test
    void testTargetSupersetRejectsMappedColumnTypeMismatch() {
        SchemaValidator validator = new TargetSupersetSchemaValidator();

        assertThatThrownBy(
                        () ->
                                validator.validateSchema(
                                        schema("id", DataTypes.INT()),
                                        schema("id", DataTypes.STRING())))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("The data type of column id is changed");
    }

    private static Schema schema(Object... columns) {
        Schema.Builder builder = Schema.newBuilder();
        for (int i = 0; i < columns.length; i += 2) {
            builder.column((String) columns[i], (DataType) columns[i + 1]);
        }
        return builder.build();
    }
}

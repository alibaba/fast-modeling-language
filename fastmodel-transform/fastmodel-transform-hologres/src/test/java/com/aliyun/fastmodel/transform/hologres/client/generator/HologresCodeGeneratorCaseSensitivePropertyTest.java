/*
 * Copyright 2026 Alibaba Group Holding Ltd.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.aliyun.fastmodel.transform.hologres.client.generator;

import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableProperties;
import com.aliyun.fastmodel.transform.api.client.CodeGenerator;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.request.DdlGeneratorModelRequest;
import com.aliyun.fastmodel.transform.api.client.dto.result.DdlGeneratorResult;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.client.dto.table.TableConfig;
import com.aliyun.fastmodel.transform.api.client.generator.DefaultCodeGenerator;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.hologres.client.converter.HologresPropertyConverter;
import com.aliyun.fastmodel.transform.hologres.context.HologresTransformContext;
import com.aliyun.fastmodel.transform.hologres.dialect.HologresVersion;
import com.aliyun.fastmodel.transform.hologres.format.HologresAstVisitor;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Regression tests for column references in Hologres table properties.
 */
public class HologresCodeGeneratorCaseSensitivePropertyTest {

    private static final List<Column> CAMEL_CASE_COLUMNS = ImmutableList.of(
        column("raceId"), column("driverId"), column("lap")
    );

    @Test
    public void testCreateV2NormalizesFiveColumnPropertiesWhenCaseInsensitive() {
        String ddl = createTable(false, HologresVersion.V2, ImmutableList.of(
            new Property("distribution_key", "raceId, driverId"),
            new Property("clustering_key", "raceId:desc,lap"),
            new Property("segment_key", "driverId"),
            new Property("bitmap_columns", "raceId:on,lap:off"),
            new Property("dictionary_encoding_columns", "raceId:auto,driverId:off,lap")
        ));

        assertTrue(ddl.contains("CALL SET_TABLE_PROPERTY('race', 'distribution_key', '\"raceid\",\"driverid\"');"));
        assertTrue(ddl.contains("CALL SET_TABLE_PROPERTY('race', 'clustering_key', '\"raceid\":DESC,\"lap\"');"));
        assertTrue(ddl.contains("CALL SET_TABLE_PROPERTY('race', 'segment_key', '\"driverid\"');"));
        assertTrue(ddl.contains("CALL SET_TABLE_PROPERTY('race', 'bitmap_columns', '\"raceid\":on,\"lap\":off');"));
        assertTrue(ddl.contains(
            "CALL SET_TABLE_PROPERTY('race', 'dictionary_encoding_columns', '\"raceid\":auto,\"driverid\":off,\"lap\":on');"));
    }

    @Test
    public void testCreateV2PreservesCaseSensitiveOutput() {
        String ddl = createTable(true, HologresVersion.V2, ImmutableList.of(
            new Property("distribution_key", "raceId,driverId"),
            new Property("dictionary_encoding_columns", "raceId:auto,lap")
        ));

        assertTrue(ddl.contains("\"raceId\""));
        assertTrue(ddl.contains("CALL SET_TABLE_PROPERTY('\"race\"', 'distribution_key', '\"raceId\",\"driverId\"');"));
        assertTrue(ddl.contains(
            "CALL SET_TABLE_PROPERTY('\"race\"', 'dictionary_encoding_columns', '\"raceId\":auto,\"lap\":on');"));
    }

    @Test
    public void testCreateV2HandlesQuotedColumnsAndWhitespace() {
        String ddl = createTable(false, HologresVersion.V2, ImmutableList.of(
            new Property("distribution_key", "\"raceId\", \"driverId\"")
        ));

        assertTrue(ddl.contains("CALL SET_TABLE_PROPERTY('race', 'distribution_key', '\"raceid\",\"driverid\"');"));
    }

    @Test
    public void testAlterCallPathFallsBackToUnquotedIdentifierNormalization() {
        String ddl = alterTable(false, "dictionary_encoding_columns", "raceId:auto, driverId", false);

        assertEquals("BEGIN;\n"
            + "CALL SET_TABLE_PROPERTY('s1.race', 'dictionary_encoding_columns', '\"raceid\":auto,\"driverid\":on');\n"
            + "COMMIT;", ddl);
    }

    @Test
    public void testAlterSetPathFallsBackToUnquotedIdentifierNormalization() {
        String ddl = alterTable(false, "clustering_key", "raceId:desc, driverId", true);

        assertEquals("BEGIN;\n"
            + "ALTER TABLE s1.race SET (clustering_key='\"raceid\":DESC,\"driverid\"');\n"
            + "COMMIT;", ddl);
    }

    @Test
    public void testV1WholeValueQuotingRemainsCompatible() {
        String ddl = createTable(false, HologresVersion.V1, ImmutableList.of(
            new Property("distribution_key", "raceId,driverId")
        ));

        assertTrue(ddl.contains("CALL SET_TABLE_PROPERTY('race', 'distribution_key', '\"raceId,driverId\"');"));
    }

    @Test
    public void testEventTimeColumnRemainsUnchanged() {
        String ddl = createTable(false, HologresVersion.V2, ImmutableList.of(
            new Property("event_time_column", "raceId")
        ));

        assertTrue(ddl.contains("CALL SET_TABLE_PROPERTY('race', 'event_time_column', 'raceId');"));
    }

    private String createTable(boolean caseSensitive, HologresVersion version, List<Property> properties) {
        List<BaseClientProperty> clientProperties = properties.stream()
            .map(property -> HologresPropertyConverter.getInstance().create(property.getName(), property.getValue()))
            .collect(Collectors.toList());
        Table table = Table.builder()
            .name("race")
            .columns(CAMEL_CASE_COLUMNS)
            .properties(clientProperties)
            .build();
        DialectMeta dialectMeta = DialectMeta.getByNameAndVersion(DialectName.HOLOGRES.getValue(), version);
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .after(table)
            .config(TableConfig.builder().dialectMeta(dialectMeta).caseSensitive(caseSensitive).build())
            .build();
        CodeGenerator codeGenerator = new DefaultCodeGenerator();
        DdlGeneratorResult result = codeGenerator.generate(request);
        return result.getDialectNodes().stream().map(DialectNode::getNode).collect(Collectors.joining("\n"));
    }

    private String alterTable(boolean caseSensitive, String key, String value, boolean useAlter) {
        HologresAstVisitor visitor = new HologresAstVisitor(HologresTransformContext.builder()
            .schema("s1")
            .caseSensitive(caseSensitive)
            .useAlterTableSetSentence(useAlter)
            .build(), HologresVersion.V2);
        visitor.visitSetTableProperties(new SetTableProperties(
            QualifiedName.of("race"), ImmutableList.of(new Property(key, value))), 0);
        return visitor.getBuilder().toString();
    }

    private static Column column(String name) {
        return Column.builder()
            .name(name)
            .dataType("int8")
            .build();
    }

    @Test
    public void testAlterCallPreservesQuotedColumnCaseWithoutDefinitions() {
        String ddl = alterTable(false, "distribution_key", " \"CamelCase\", PlainColumn ", false);
        assertEquals("BEGIN;\n"
            + "CALL SET_TABLE_PROPERTY('s1.race', 'distribution_key', '\"CamelCase\",\"plaincolumn\"');\n"
            + "COMMIT;", ddl);
    }

    @Test
    public void testAlterSetPreservesQuotedColumnCaseWithoutDefinitions() {
        String ddl = alterTable(false, "clustering_key", "\"CamelCase\":desc, PlainColumn", true);
        assertEquals("BEGIN;\n"
            + "ALTER TABLE s1.race SET (clustering_key='\"CamelCase\":DESC,\"plaincolumn\"');\n"
            + "COMMIT;", ddl);

        String callDdl = alterTable(false, "clustering_key", "\"CamelCase\":desc, PlainColumn", false);
        assertEquals("BEGIN;\n"
            + "CALL SET_TABLE_PROPERTY('s1.race', 'clustering_key', '\"CamelCase\":DESC,\"plaincolumn\"');\n"
            + "COMMIT;", callDdl);
    }

    @Test
    public void testAlterPreservesQuotedColumnCaseInColumnStatusProperties() {
        for (String key : ImmutableList.of("bitmap_columns", "dictionary_encoding_columns")) {
            String ddl = alterTable(false, key, "\"CamelCase\":on, PlainColumn:off", true);
            assertEquals("BEGIN;\n"
                + "ALTER TABLE s1.race SET (" + key + "='\"CamelCase\":on,\"plaincolumn\":off');\n"
                + "COMMIT;", ddl);

            String callDdl = alterTable(false, key, "\"CamelCase\":on, PlainColumn:off", false);
            assertEquals("BEGIN;\n"
                + "CALL SET_TABLE_PROPERTY('s1.race', '" + key + "', '\"CamelCase\":on,\"plaincolumn\":off');\n"
                + "COMMIT;", callDdl);
        }
    }
}

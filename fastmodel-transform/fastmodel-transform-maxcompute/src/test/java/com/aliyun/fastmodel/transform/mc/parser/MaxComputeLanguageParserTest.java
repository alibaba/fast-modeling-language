/*
 * Copyright 2021 Alibaba Group Holding Ltd.
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

package com.aliyun.fastmodel.transform.mc.parser;

import java.util.List;

import com.aliyun.fastmodel.core.exception.ParseException;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.core.tree.statement.constants.TableDetailType;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateAdsTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDimTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDwsTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateFactTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.context.ReverseContext.ReverseTargetStrategy;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/9/7
 */
public class MaxComputeLanguageParserTest {

    MaxComputeLanguageParser maxComputeLanguageParser = new MaxComputeLanguageParser();

    @Test
    public void parseNode() {
        CreateDimTable o = maxComputeLanguageParser.parseNode("create table a (b bigint) comment 'comment'");
        assertEquals(o.getQualifiedName(), QualifiedName.of("a"));
        List<ColumnDefinition> columnDefines = o.getColumnDefines();
        assertEquals(columnDefines.size(), 1);
    }

    @Test
    public void testParseStruct() {
        CreateDimTable createDimTable = maxComputeLanguageParser.parseNode("create table a (b ARRAY<STRUCT<item_id:STRING,item_name:STRING>>)");
        assertEquals(createDimTable.getColumnDefines().size(), 1);
        ColumnDefinition columnDefinition = createDimTable.getColumnDefines().get(0);
        BaseDataType dataType = columnDefinition.getDataType();
        assertNotNull(dataType);
    }

    @Test
    public void testReverse() {
        CreateDimTable table =
            maxComputeLanguageParser.parseNode("create table a (`b` bigint comment 'comment') comment 'comment'");
        assertEquals(table.getColumnDefines().size(), 1);
        ColumnDefinition columnDefinition = table.getColumnDefines().get(0);
        assertEquals(columnDefinition.getColName().getValue(), "b");
    }

    @Test
    public void testReverseCostColumnName() {
        CreateDimTable table = maxComputeLanguageParser.parseNode("create table a (cost bigint comment 'cost column')");
        assertEquals(table.getColumnDefines().size(), 1);
        ColumnDefinition columnDefinition = table.getColumnDefines().get(0);
        assertEquals("cost", columnDefinition.getColName().getValue());
    }

    @Test
    public void testReverseAds() {
        Node node = maxComputeLanguageParser.parseNode("create table a (b bigint comment 'comment') comment 'ba'",
            ReverseContext.builder().reverseTableType(TableDetailType.ADS).build());
        assertEquals(node.getClass(), CreateAdsTable.class);
    }

    @Test
    public void testReverseFact() {
        CreateFactTable table =
            (CreateFactTable)maxComputeLanguageParser.parseNode("create table a (`b` bigint comment 'comment') comment 'comment'",
                ReverseContext.builder().reverseTableType(TableDetailType.TRANSACTION_FACT).build());
        assertEquals(table.getColumnDefines().size(), 1);
        ColumnDefinition columnDefinition = table.getColumnDefines().get(0);
        assertEquals(columnDefinition.getColName().getValue(), "b");
    }

    @Test
    public void testReverseDws() {
        CreateDwsTable table =
            (CreateDwsTable)maxComputeLanguageParser.parseNode("create table a (`b` bigint comment 'comment') comment 'comment'",
                ReverseContext.builder().reverseTableType(TableDetailType.DWS).build());
        assertEquals(table.getColumnDefines().size(), 1);
        ColumnDefinition columnDefinition = table.getColumnDefines().get(0);
        assertEquals(columnDefinition.getColName().getValue(), "b");
    }

    @Test
    public void testWithProperty() {
        CreateDwsTable table =
            (CreateDwsTable)maxComputeLanguageParser.parseNode(
                "create table a (`b` bigint comment 'comment') comment 'comment' tblproperties('a'='b')",
                ReverseContext.builder().reverseTableType(TableDetailType.DWS).build());
        List<Property> properties = table.getProperties();
        assertEquals(1, properties.size());
        assertEquals(properties.get(0).getName(), "a");
        assertEquals(properties.get(0).getValue(), "b");
    }

    @Test
    public void testParseDataType() {
        String dataType = "array<string>";
        BaseDataType baseDataType = maxComputeLanguageParser.parseDataType(dataType, ReverseContext.builder().build());
        assertNotNull(baseDataType);
    }

    @Test
    public void testReverseSupportNtz() {
        String dataType = "timestamp_ntz";
        BaseDataType baseDataType = maxComputeLanguageParser.parseDataType(dataType, ReverseContext.builder().build());
        assertNotNull(baseDataType);
        assertEquals("TIMESTAMP_NTZ", baseDataType.getTypeName().getValue());
    }

    @Test
    public void testReverseSupportVector() {
        String dataType = "VECTOR(FLOAT, 768)";
        BaseDataType baseDataType = maxComputeLanguageParser.parseDataType(dataType, ReverseContext.builder().build());
        assertNotNull(baseDataType);
        assertEquals("VECTOR", baseDataType.getTypeName().getValue());
        assertEquals("VECTOR(FLOAT,768)", baseDataType.toString());
    }

    @Test
    public void testReverseSupportVectorBoundariesAndFormatting() {
        assertEquals("VECTOR(FLOAT,1)", maxComputeLanguageParser
            .parseDataType(" vector ( float , 1 ) ", ReverseContext.builder().build()).toString());
        assertEquals("VECTOR(FLOAT,4096)", maxComputeLanguageParser
            .parseDataType("VECTOR(FLOAT,4096)", ReverseContext.builder().build()).toString());
    }

    @Test
    public void testReverseRejectsInvalidVectorTypesAndDimensions() {
        String[] invalid = {"VECTOR(STRING,1)", "VECTOR(FLOAT,1.5)", "VECTOR(FLOAT,0)",
            "VECTOR(FLOAT,-1)", "VECTOR(FLOAT,4097)", "VECTOR(FLOAT)", "VECTOR(FLOAT,1,2)"};
        for (String expression : invalid) {
            try {
                maxComputeLanguageParser.parseDataType(expression, ReverseContext.builder().build());
                fail("Expected VECTOR validation failure for " + expression);
            } catch (RuntimeException expected) {
                // expected
            }
        }
    }

    @Test
    public void testReverseCompleteCreateTableWithVectorColumn() {
        String sql = "CREATE TABLE IF NOT EXISTS project.schema.table\n"
            + "(\n"
            + "   col1 STRING COMMENT '',\n"
            + "   col2 STRING COMMENT '',\n"
            + "   col3 MAP<STRING,STRUCT<x:DECIMAL(38,10),y:VARCHAR(10)>> COMMENT '',\n"
            + "   col4 ARRAY<VARCHAR(10)> COMMENT '',\n"
            + "   col5 STRUCT<x:VARCHAR(10),y:CHAR(10)> COMMENT '',\n"
            + "   col6 VECTOR(FLOAT,3) COMMENT ''\n"
            + ")\n"
            + "COMMENT 'created by DI'\n"
            + "PARTITIONED BY\n"
            + "(\n"
            + "   pt STRING COMMENT '',\n"
            + "   ds STRING COMMENT ''\n"
            + ")\n"
            + "LIFECYCLE 36500;";

        CreateDimTable table = maxComputeLanguageParser.parseNode(sql);
        assertEquals(6, table.getColumnDefines().size());
        assertEquals("VECTOR(FLOAT,3)", table.getColumnDefines().get(5).getDataType().toString());
        assertEquals(2, table.getPartitionedBy().getColumnDefinitions().size());
    }

    @Test
    public void testComplexStructWithKeywords() {
        String ddl
            = "create table t1 (membership_tier STRUCT<current:STRING,is_tier_maintained:BOOLEAN,next_tier:STRING,progress_message:STRING,"
            + "message_type:STRING,message:STRING,badge_url:STRING,upgrades_to:STRING,maintains_tier:STRING>) comment 'abc';";
        CreateTable o = maxComputeLanguageParser.parseNode(ddl);
        assertNotNull(o);
    }

    @Test
    public void scriptPreservesEveryStatement() {
        CompositeStatement script = (CompositeStatement)maxComputeLanguageParser.parseNode(
            "CREATE TABLE example_alpha (id BIGINT); CREATE TABLE example_beta (id BIGINT);",
            ReverseContext.builder().reverseTargetStrategy(ReverseTargetStrategy.SCRIPT).build());
        assertEquals(2, script.getStatements().size());
        assertEquals(QualifiedName.of("example_alpha"),
            ((CreateTable)script.getStatements().get(0)).getQualifiedName());
        assertEquals(QualifiedName.of("example_beta"),
            ((CreateTable)script.getStatements().get(1)).getQualifiedName());
    }

    @Test
    public void scriptAcceptsFinalStatementWithoutTerminatorBeforeComment() {
        CompositeStatement script = (CompositeStatement)maxComputeLanguageParser.parseNode(
            "CREATE TABLE example_alpha (id BIGINT); CREATE TABLE example_beta (id BIGINT) -- comment",
            ReverseContext.builder().reverseTargetStrategy(ReverseTargetStrategy.SCRIPT).build());
        assertEquals(2, script.getStatements().size());
        assertEquals(QualifiedName.of("example_beta"),
            ((CreateTable)script.getStatements().get(1)).getQualifiedName());
    }

    @Test(expected = ParseException.class)
    public void scriptRejectsInvalidTrailingStatement() {
        maxComputeLanguageParser.parseNode("CREATE TABLE example_alpha (id BIGINT); THIS IS NOT SQL;",
            ReverseContext.builder().reverseTargetStrategy(ReverseTargetStrategy.SCRIPT).build());
    }

    @Test(expected = ParseException.class)
    public void singleStatementRejectsTrailingGarbage() {
        maxComputeLanguageParser.parseNode("CREATE TABLE example_alpha (id BIGINT); THIS IS NOT SQL;");
    }

    @Test(expected = ParseException.class)
    public void singleStatementRejectsASecondStatement() {
        maxComputeLanguageParser.parseNode(
            "CREATE TABLE example_alpha (id BIGINT); CREATE TABLE example_beta (id BIGINT);");
    }

    @Test
    public void singleStatementAcceptsTerminatorsAndTrailingComments() {
        for (String suffix : new String[] {"", ";", ";  ", " -- comment", "; -- comment", "; /* comment */"}) {
            CreateTable table = maxComputeLanguageParser.parseNode("CREATE TABLE example_alpha (id BIGINT)" + suffix);
            assertEquals(QualifiedName.of("example_alpha"), table.getQualifiedName());
        }
    }

    @Test(expected = ParseException.class)
    public void dataTypeRejectsTrailingTokens() {
        maxComputeLanguageParser.parseDataType("BIGINT unexpected", ReverseContext.builder().build());
    }

    @Test(expected = ParseException.class)
    public void expressionRejectsTrailingTokens() {
        maxComputeLanguageParser.parseExpression("1 + 2 unexpected");
    }
}

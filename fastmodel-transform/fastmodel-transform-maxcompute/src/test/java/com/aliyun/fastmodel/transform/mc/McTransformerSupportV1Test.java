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

package com.aliyun.fastmodel.transform.mc;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeParameter;
import com.aliyun.fastmodel.core.tree.datatype.TypeParameter;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.element.CreateElement;
import com.aliyun.fastmodel.core.tree.statement.rule.CreateRules;
import com.aliyun.fastmodel.core.tree.statement.rule.RulesLevel;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDimTable;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableProperties;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.parser.NodeParser;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.mc.context.MaxComputeContext;
import com.aliyun.fastmodel.transform.mc.format.MaxComputePropertyKey;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/2/19
 */
public class McTransformerSupportV1Test {

    MaxComputeTransformerSupportV1 mcTransformerSupportV1 = new MaxComputeTransformerSupportV1();

    @Test
    public void testTransform() {
        ImmutableList<ColumnDefinition> abc = ImmutableList.of(
            ColumnDefinition.builder().colName(new Identifier("abc"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.VARCHAR)).build()
        );
        CreateDimTable abc2 = CreateDimTable.builder().tableName(QualifiedName.of("a.b")).columns(abc).comment(
            new Comment("abc")
        ).build();
        DialectNode abc1 = mcTransformerSupportV1.transform(
            abc2);
        assertEquals(abc1.getNode(), "CREATE TABLE b\n"
            + "(\n"
            + "   abc STRING\n"
            + ")\n"
            + "COMMENT 'abc'");
    }

    @Test
    public void testTransformChar() {
        ImmutableList<ColumnDefinition> abc = ImmutableList.of(
            ColumnDefinition.builder().colName(new Identifier("abc"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.CHAR)).build()
        );
        CreateDimTable abc2 = CreateDimTable.builder().tableName(QualifiedName.of("a.b")).columns(
            abc
        ).comment(new Comment("abc")).build();
        DialectNode abc1 = mcTransformerSupportV1.transform(
            abc2);
        assertEquals(abc1.getNode(), "CREATE TABLE b\n"
            + "(\n"
            + "   abc STRING\n"
            + ")\n"
            + "COMMENT 'abc'");
    }

    @Test
    public void testTransformMap() {
        ImmutableList<DataTypeParameter> of = ImmutableList.of(
            new TypeParameter(DataTypeUtil.simpleType(DataTypeEnums.BIGINT)),
            new TypeParameter(DataTypeUtil.simpleType(DataTypeEnums.STRING))
        );
        BaseDataType dataType = DataTypeUtil.simpleType(DataTypeEnums.MAP, of.toArray(new DataTypeParameter[0]));
        ImmutableList<ColumnDefinition> abc = ImmutableList.of(
            ColumnDefinition.builder().colName(new Identifier("abc"))
                .dataType(dataType).build()
        );
        CreateDimTable abc2 = CreateDimTable.builder().tableName(QualifiedName.of("a.b")).columns(
            abc
        ).comment(new Comment("abc")).build();
        DialectNode abc1 = mcTransformerSupportV1.transform(
            abc2);
        assertEquals(abc1.getNode(), "CREATE TABLE b\n"
            + "(\n"
            + "   abc MAP<BIGINT,STRING>\n"
            + ")\n"
            + "COMMENT 'abc'");
    }

    @Test
    public void testTransformInt() {
        ImmutableList<ColumnDefinition> abc = ImmutableList.of(
            ColumnDefinition.builder().colName(new Identifier("abc"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.SMALLINT)).build()
        );
        CreateDimTable abc2 = CreateDimTable.builder().tableName(QualifiedName.of("a.b")).columns(abc).comment(
            new Comment("abc")
        ).build();
        DialectNode abc1 = mcTransformerSupportV1.transform(
            abc2);
        assertEquals(abc1.getNode(), "CREATE TABLE b\n"
            + "(\n"
            + "   abc BIGINT\n"
            + ")\n"
            + "COMMENT 'abc'");
    }

    @Test
    public void testTransformJson() {
        ImmutableList<ColumnDefinition> abc = ImmutableList.of(
            ColumnDefinition.builder().colName(new Identifier("abc"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.JSON)).build()
        );
        CreateDimTable abc2 = CreateDimTable.builder().tableName(QualifiedName.of("a.b")).columns(abc).comment(
            new Comment("abc")
        ).build();
        DialectNode abc1 = mcTransformerSupportV1.transform(
            abc2);
        assertEquals(abc1.getNode(), "CREATE TABLE b\n"
            + "(\n"
            + "   abc JSON\n"
            + ")\n"
            + "COMMENT 'abc'");
    }

    @Test
    public void testTransformProperties() {
        ImmutableList<ColumnDefinition> abc = ImmutableList.of(
            ColumnDefinition.builder().colName(new Identifier("abc"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.SMALLINT)).build()
        );
        CreateDimTable abc2 = CreateDimTable.builder().tableName(QualifiedName.of("a.b")).columns(abc).comment(
            new Comment("abc")
        ).properties(ImmutableList.of(new Property(MaxComputePropertyKey.LIFE_CYCLE.getValue(), "10"))).build();
        DialectNode abc1 = mcTransformerSupportV1.transform(
            abc2);
        assertEquals(abc1.getNode(), "CREATE TABLE b\n"
            + "(\n"
            + "   abc BIGINT\n"
            + ")\n"
            + "COMMENT 'abc'\nLIFECYCLE 10");

        abc2 = CreateDimTable.builder().tableName(QualifiedName.of("a.b")).columns(abc).comment(
            new Comment("abc")
        ).build();
        abc1 = mcTransformerSupportV1.transform(
            abc2, MaxComputeContext.builder().build());
        assertEquals(abc1.getNode(), "CREATE TABLE b\n"
            + "(\n"
            + "   abc BIGINT\n"
            + ")\n"
            + "COMMENT 'abc'");
    }

    @Test
    public void testSetProperties() {
        SetTableProperties setTableProperties = new SetTableProperties(QualifiedName.of("dim_shop"),
            ImmutableList.of(new Property(MaxComputePropertyKey.LIFE_CYCLE.getValue(), "10")));
        DialectNode transform = mcTransformerSupportV1.transform(setTableProperties,
            MaxComputeContext.builder().build());
        assertEquals(transform.isExecutable(), true);
        assertEquals(transform.getNode(), "ALTER TABLE dim_shop ENABLE LIFECYCLE;\n"
            + "ALTER TABLE dim_shop SET LIFECYCLE 10");
    }

    @Test
    public void testSetPropertiesWithoutLifecycle() {
        SetTableProperties setTableProperties = new SetTableProperties(QualifiedName.of("dim_shop"),
            ImmutableList.of(new Property("key", "10")));
        DialectNode transform = mcTransformerSupportV1.transform(setTableProperties,
            MaxComputeContext.builder().build());
        assertEquals(transform.isExecutable(), false);
    }

    @Test
    public void testTransformCreateRules() {
        DialectNode qu = mcTransformerSupportV1.transform(
            CreateRules.builder().ruleLevel(RulesLevel.TASK).tableName(QualifiedName.of("dim_shop"))
                .createElement(CreateElement.builder().qualifiedName(QualifiedName.of("qu")).build())
                .build());
        assertNotNull(qu.getNode());
    }

    @Test
    public void convertsNestedFmlTypesWithoutChangingV2OrSourceAst() {
        BaseStatement statement = new NodeParser().parseNode("create dim table example_types ("
            + "example_int int, example_array array<smallint>, "
            + "example_map map<string,struct<example_count:int comment 'Synthetic count',"
            + "example_value:varchar(10),example_ratio:decimal(8,2)>>);");
        MaxComputeTransformer v2 = new MaxComputeTransformer();
        String originalV2 = v2.transform(statement).getNode();
        String v1Sql = mcTransformerSupportV1.transform(statement).getNode().replaceAll("\\s+", " ");
        assertTrue(v1Sql, v1Sql.contains("example_int BIGINT"));
        assertTrue(v1Sql, v1Sql.contains("example_array ARRAY<BIGINT>"));
        assertTrue(v1Sql, v1Sql.contains("MAP<STRING,STRUCT<example_count:BIGINT COMMENT 'Synthetic count',"
            + "example_value:STRING,example_ratio:DECIMAL(8,2)>>"));
        assertEquals(originalV2, v2.transform(statement).getNode());
        assertTrue(originalV2, originalV2.contains("ARRAY<SMALLINT>"));
        assertTrue(originalV2, originalV2.contains("example_value:VARCHAR(10)"));
    }

    @Test
    public void convertsNativeTypesIncludingNestedContainers() {
        BaseStatement statement = new MaxComputeTransformer().reverse(new DialectNode(
            "CREATE TABLE example_native_types (example_small SMALLINT, example_int INT, "
                + "example_array ARRAY<SMALLINT>, example_map MAP<STRING,STRUCT<example_count:INT,"
                + "example_value:VARCHAR(10)>>);"));
        String sql = mcTransformerSupportV1.transform(statement).getNode().replaceAll("\\s+", " ");
        assertTrue(sql, sql.contains("example_small BIGINT"));
        assertTrue(sql, sql.contains("example_int BIGINT"));
        assertTrue(sql, sql.contains("example_array ARRAY<BIGINT>"));
        assertTrue(sql, sql.contains("MAP<STRING,STRUCT<example_count:BIGINT,example_value:STRING>>"));
    }
}

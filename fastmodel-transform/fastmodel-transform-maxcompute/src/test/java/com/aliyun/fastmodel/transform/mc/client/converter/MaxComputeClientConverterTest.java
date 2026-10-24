/*
 * Copyright 2022 Alibaba Group Holding Ltd.
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

package com.aliyun.fastmodel.transform.mc.client.converter;

import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeParameter;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.statement.constants.TableDetailType;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDwsTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.transform.api.client.PropertyConverter;
import com.aliyun.fastmodel.transform.api.client.dto.constraint.Constraint;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.client.dto.table.TableConfig;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.mc.MaxComputeTransformer;
import com.aliyun.fastmodel.transform.mc.client.property.LifeCycle;
import com.aliyun.fastmodel.transform.mc.context.MaxComputeContext;
import com.aliyun.fastmodel.transform.mc.format.MaxComputeFormatter;
import com.aliyun.fastmodel.transform.mc.parser.MaxComputeLanguageParser;
import com.aliyun.fastmodel.transform.mc.parser.tree.datatype.MaxComputeDataTypeName;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/8/5
 */
public class MaxComputeClientConverterTest {

    MaxComputeClientConverter maxComputeClientConverter = new MaxComputeClientConverter();

    @Test
    public void getPropertyConverter() {
        PropertyConverter propertyConverter = maxComputeClientConverter.getPropertyConverter();
        LifeCycle baseClientProperty = (LifeCycle)propertyConverter.create(LifeCycle.LIFECYCLE, "10");
        assertEquals(baseClientProperty.getKey(), LifeCycle.LIFECYCLE);
        assertEquals(baseClientProperty.getValue(), new Long(10L));
    }

    @Test
    public void toLifeCycle() {
        CreateTable createTable = CreateTable.builder()
            .properties(Lists.newArrayList())
            .build();
        Long lifeCycleSeconds = maxComputeClientConverter.toLifeCycleSeconds(createTable);
        assertNull(lifeCycleSeconds);

        List<Property> properties = Lists.newArrayList(
            new Property(LifeCycle.LIFECYCLE, "10")
        );
        CreateTable createTable1 = CreateTable.builder()
            .properties(properties)
            .build();
        lifeCycleSeconds = maxComputeClientConverter.toLifeCycleSeconds(createTable1);
        assertEquals(lifeCycleSeconds, new Long(864000L));
    }

    @Test
    public void getDataTypeZero() {
        Column column = Column.builder()
            .name("c1")
            .dataType("bigint")
            .comment("comment")
            .build();
        BaseDataType dataType = maxComputeClientConverter.getDataType(column);
        assertEquals(dataType.getTypeName(), DataTypeEnums.BIGINT);
    }

    @Test
    public void getDataTypeOne() {
        Column column = Column.builder()
            .name("c1")
            .dataType("varchar")
            .length(1)
            .comment("comment")
            .build();
        BaseDataType dataType = maxComputeClientConverter.getDataType(column);
        assertEquals(dataType.getTypeName(), MaxComputeDataTypeName.VARCHAR);
        GenericDataType genericDataType = (GenericDataType)dataType;
        List<DataTypeParameter> arguments = genericDataType.getArguments();
        assertEquals(arguments.size(), 1);
    }

    @Test
    public void getDataTypeTwo() {
        Column column = Column.builder()
            .name("c1")
            .dataType("decimal")
            .precision(1)
            .scale(1)
            .comment("comment")
            .build();
        BaseDataType dataType = maxComputeClientConverter.getDataType(column);
        assertEquals(dataType.getTypeName(), MaxComputeDataTypeName.DECIMAL);
        GenericDataType genericDataType = (GenericDataType)dataType;
        List<DataTypeParameter> arguments = genericDataType.getArguments();
        assertEquals(arguments.size(), 2);
    }

    @Test
    public void getDataTypeDecimal() {
        Column column = Column.builder()
            .name("c1")
            .dataType("decimal(10,2)")
            .comment("comment")
            .build();
        BaseDataType dataType = maxComputeClientConverter.getDataType(column);
        assertEquals(dataType.getTypeName(), MaxComputeDataTypeName.DECIMAL);
        GenericDataType genericDataType = (GenericDataType)dataType;
        List<DataTypeParameter> arguments = genericDataType.getArguments();
        assertEquals(arguments.size(), 2);
    }

    @Test
    public void getDataTypeM() {
        Column column = Column.builder()
            .name("c1")
            .dataType("array<string>")
            .precision(1)
            .scale(1)
            .comment("comment")
            .build();
        BaseDataType dataType = maxComputeClientConverter.getDataType(column);
        assertEquals(dataType.getTypeName(), MaxComputeDataTypeName.ARRAY);
        GenericDataType genericDataType = (GenericDataType)dataType;
        List<DataTypeParameter> arguments = genericDataType.getArguments();
        assertEquals(arguments.size(), 1);
    }

    @Test
    public void getDataTypeJson() {
        Column column = Column.builder()
            .name("c1")
            .dataType("json")
            .precision(1)
            .scale(1)
            .comment("comment")
            .build();
        BaseDataType dataType = maxComputeClientConverter.getDataType(column);
        assertEquals(dataType.getTypeName().getValue(), MaxComputeDataTypeName.JSON.getValue());
    }

    @Test
    public void getDataTypeJsonWithParam() {
        Column column = Column.builder()
            .name("c1")
            .dataType("json<a:string>")
            .precision(1)
            .scale(1)
            .comment("comment")
            .build();
        BaseDataType dataType = maxComputeClientConverter.getDataType(column);
        assertEquals(dataType.getTypeName().getValue(), MaxComputeDataTypeName.JSON.getValue());
    }

    @Test(expected = IllegalArgumentException.class)
    public void getDataTypeMap() {
        Column column = Column.builder()
            .name("c1")
            .dataType("MAP")
            .precision(1)
            .scale(1)
            .comment("comment")
            .build();
        BaseDataType dataType = maxComputeClientConverter.getDataType(column);
        assertEquals(dataType.getTypeName(), DataTypeEnums.ARRAY);
        GenericDataType genericDataType = (GenericDataType)dataType;
        List<DataTypeParameter> arguments = genericDataType.getArguments();
        assertEquals(arguments.size(), 0);
    }

    @Test
    public void testConvert() {
        List<Column> columns = Lists.newArrayList();
        columns.add(Column.builder()
            .name("a")
            .primaryKey(true)
            .dataType("string")
            .comment("commentA")
            .build()
        );
        columns.add(Column.builder()
            .name("b")
            .partitionKeyIndex(0)
            .partitionKey(true)
            .dataType("bigint")
            .comment("commentB")
            .build()
        );
        List<Constraint> constraints = Lists.newArrayList();
        Table build = Table.builder()
            .name("t1")
            .columns(columns)
            .constraints(constraints)
            .comment("comment")
            .build();
        Node node = maxComputeClientConverter.convertToNode(build, TableConfig.builder().build());
        CreateTable createTable = (CreateTable)node;
        assertEquals(createTable.getQualifiedName(), QualifiedName.of("t1"));
        assertEquals(createTable.getCommentValue(), "comment");
        List<ColumnDefinition> columnDefines = createTable.getColumnDefines();
        assertEquals(columnDefines.size(), 1);
        ColumnDefinition columnDefinition = columnDefines.get(0);
        assertEquals(columnDefinition.getCommentValue(), "commentA");
        List<ColumnDefinition> columnDefinitions = createTable.getPartitionedBy().getColumnDefinitions();
        assertEquals(1, columnDefinitions.size());
    }

    @Test
    public void testToSchema() {
        CreateTable createTable = CreateTable.builder()
            .tableName(QualifiedName.of("a.b.c"))
            .build();
        MaxComputeContext transformContext = MaxComputeContext.builder()
            .schema("s1")
            .build();
        String c = maxComputeClientConverter.toSchema(createTable, transformContext);
        assertEquals("b", c);
    }

    @Test
    public void testToSchema2() {
        CreateTable createTable = CreateTable.builder()
            .tableName(QualifiedName.of("a.c"))
            .build();
        MaxComputeContext transformContext = MaxComputeContext.builder()
            .build();
        String c = maxComputeClientConverter.toSchema(createTable, transformContext);
        assertNull(c);
    }

    @Test
    public void testClusteredBy() {
        MaxComputeTransformer maxComputeTransformer = new MaxComputeTransformer();
        BaseStatement from = maxComputeTransformer.reverse(
                new DialectNode("CREATE TABLE test_odps\n" +
                        "(\n" +
                        "    id     INT NOT NULL  PRIMARY COMMENT '唯一标识'\n" +
                        "    ,name  STRING NOT NULL DEFAULT '默认' COMMENT '名称'\n" +
                        "    ,price DECIMAL(20,4) NOT NULL COMMENT '价格'\n" +
                        "    ,age   MAP<STRING,STRING> COMMENT '复杂'\n" +
                        ")\n" +
                        "COMMENT '测试'\n" +
                        "PARTITIONED BY \n" +
                        "(\n" +
                        "    pt     STRING COMMENT '日期'\n" +
                        ")\n" +
                        "CLUSTERED BY (id)\n" +
                        "SORTED BY (id ASC) INTO 5 BUCKETS\n" +
                        "LIFECYCLE 365\n" +
                        ";"),
                ReverseContext.builder().reverseTableType(TableDetailType.DWS).build()
        );
        assertTrue(from instanceof CreateDwsTable);
        CreateDwsTable createDwsTable = (CreateDwsTable) from;

        MaxComputeContext transformContext = MaxComputeContext.builder()
                .build();
        Table table = maxComputeClientConverter.convertToTable(createDwsTable, transformContext);
        assertNotNull(table);

        TableConfig build = TableConfig.builder().build();
        Node node = maxComputeClientConverter.convertToNode(table, build);
        assertNotNull(node);
    }

    @Test
    public void testVectorRoundTripWithOrdinaryClassLoader() {
        MaxComputeClientConverter converter = new MaxComputeClientConverter();
        CreateTable parsed = new MaxComputeLanguageParser()
            .parseNode("CREATE TABLE ordinary_vector (embedding VECTOR(FLOAT,3));");

        Table table = converter.convertToTable(parsed, MaxComputeContext.builder().build());
        assertEquals("VECTOR(FLOAT,3)", table.getColumns().get(0).getDataType());

        CreateTable roundTrip = (CreateTable)converter.convertToNode(table, TableConfig.builder().build());
        DialectNode ddl = MaxComputeFormatter.formatSql(roundTrip, MaxComputeContext.builder().build());
        assertTrue(ddl.getNode().contains("VECTOR(FLOAT,3)"));
        assertEquals("VECTOR(FLOAT,3)", roundTrip.getColumnDefines().get(0).getDataType().toString());
    }

}

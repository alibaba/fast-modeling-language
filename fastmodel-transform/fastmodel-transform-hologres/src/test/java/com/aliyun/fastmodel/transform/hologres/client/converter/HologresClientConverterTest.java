/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.client.converter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.datatype.NumericParameter;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.UniqueConstraint;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.api.client.dto.constraint.Constraint;
import com.aliyun.fastmodel.transform.api.client.dto.constraint.OutlineConstraintType;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.client.dto.table.TableConfig;
import com.aliyun.fastmodel.transform.hologres.client.property.ClusterKey;
import com.aliyun.fastmodel.transform.hologres.client.property.DictEncodingColumn;
import com.aliyun.fastmodel.transform.hologres.client.property.ColumnStatus;
import com.aliyun.fastmodel.transform.hologres.client.property.Status;
import com.aliyun.fastmodel.transform.hologres.client.property.DistributionKey;
import com.aliyun.fastmodel.transform.hologres.client.property.TimeToLiveSeconds;
import com.aliyun.fastmodel.transform.hologres.context.HologresTransformContext;
import com.aliyun.fastmodel.transform.hologres.parser.tree.datatype.HologresDataTypeName;
import com.aliyun.fastmodel.transform.hologres.parser.tree.datatype.HologresGenericDataType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * HologresClientConverterTest
 *
 * @author panguanjing
 * @date 2022/6/7
 */
public class HologresClientConverterTest {

    HologresClientConverter hologresClientConverter;

    @Before
    public void setUp() throws Exception {
        hologresClientConverter = new HologresClientConverter();
    }

    @Test
    public void covertToNode() {
        List<Column> columns = new ArrayList<>();
        columns.add(
            Column.builder()
                .name("name")
                .dataType("text")
                .comment("comment")
                .build()
        );
        columns.add(
            Column.builder()
                .name("p1")
                .dataType("varchar")
                .length(2)
                .nullable(false)
                .partitionKey(true)
                .partitionKeyIndex(0)
                .build()
        );

        columns.add(
            Column.builder()
                .name("p2")
                .dataType("varchar")
                .length(2)
                .nullable(false)
                .primaryKey(true)
                .build()
        );
        List<BaseClientProperty> properties = getPropertys();
        List<Constraint> constraints = getConstraints();
        Table table = Table.builder()
            .database("database")
            .columns(columns)
            .name("name")
            .lifecycleSeconds(1000L)
            .properties(properties)
            .constraints(constraints)
            .build();
        CreateTable node = (CreateTable)hologresClientConverter.covertToNode(table, TableConfig.builder().build());
        assertEquals(node.isNotExists(), true);
        assertEquals(node.getQualifiedName().toString(), "database.name");
        assertEquals(node.getColumnDefines().size(), 3);
        assertEquals(node.getPartitionedBy().getColumnDefinitions().size(), 1);
        assertEquals(node.getProperties().size(), 3);
        assertEquals(node.getProperties().get(0).getValue(), "1000");
        assertEquals(node.getProperties().get(1).getValue(), "c1:auto");
        assertEquals(node.getProperties().get(2).getValue(), "a,b");
        assertEquals(node.getConstraintStatements().size(), 1);
    }

    @Test
    public void testConvertWithScale() {
        List<Column> columns = new ArrayList<>();
        columns.add(
            Column.builder()
                .name("name")
                .dataType("decimal")
                .comment("comment")
                .precision(10)
                .scale(38)
                .build()
        );
        Table table = Table.builder()
            .database("database")
            .schema("public")
            .columns(columns)
            .name("name")
            .build();
        Node node = hologresClientConverter.covertToNode(table, TableConfig.builder().build());
        CreateTable createTable = (CreateTable)node;
        List<ColumnDefinition> columnDefines = createTable.getColumnDefines();
        assertEquals(columnDefines.get(0).getDataType(),
            new HologresGenericDataType(HologresDataTypeName.DECIMAL.getValue(),
                Arrays.asList(
                    new NumericParameter("10"),
                    new NumericParameter("38")
                )));
    }

    private List<Constraint> getConstraints() {
        Constraint constraint = new Constraint();
        constraint.setType(OutlineConstraintType.PRIMARY_KEY);
        constraint.setName("n1");
        constraint.setColumns(Lists.newArrayList("p1"));
        return Lists.newArrayList(constraint);
    }

    private List<BaseClientProperty> getPropertys() {
        DictEncodingColumn dictEncodingColumns = new DictEncodingColumn();
        List<ColumnStatus> values = Lists.newArrayList(
            ColumnStatus.builder()
                .columnName("c1")
                .status(Status.AUTO)
                .build()
        );
        dictEncodingColumns.setValue(values);
        DistributionKey distributionKey = new DistributionKey();
        distributionKey.setValue(Arrays.asList("a", "b"));
        return Lists.newArrayList(dictEncodingColumns, distributionKey);
    }

    @Test
    public void convertToTable() {
        List<ColumnDefinition> defines = Lists.newArrayList();
        defines.add(ColumnDefinition.builder()
            .colName(new Identifier("c1"))
            .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
            .build());
        List<Property> properties = new ArrayList<>();
        properties.add(new Property(ClusterKey.CLUSTERING_KEY, "c1"));
        properties.add(new Property(TimeToLiveSeconds.TIME_TO_LIVE_IN_SECONDS, "100"));
        Node table = CreateTable.builder()
            .tableName(QualifiedName.of("a.b"))
            .comment(new Comment("abc"))
            .columns(defines)
            .properties(properties)
            .build();
        Table convertToTable = hologresClientConverter.convertToTable(table, HologresTransformContext.builder().build());
        assertNotNull(convertToTable);
        assertEquals(convertToTable.getName(), "b");
        assertEquals(convertToTable.getComment(), "abc");
        assertEquals(convertToTable.getProperties().size(), 2);
        assertEquals(convertToTable.getLifecycleSeconds(), new Long(100L));
    }

    @Test
    public void convertToTableComplexDataType() {
        List<ColumnDefinition> defines = Lists.newArrayList();
        defines.add(ColumnDefinition.builder()
            .colName(new Identifier("c1"))
            .dataType(DataTypeUtil.simpleType(DataTypeEnums.VARCHAR, new NumericParameter("10")))
            .comment(new Comment("comment"))
            .primary(true)
            .notNull(false)
            .build());
        Node table = CreateTable.builder()
            .tableName(QualifiedName.of("a.b"))
            .columns(defines)
            .build();
        Table convertToTable = hologresClientConverter.convertToTable(table, HologresTransformContext.builder().build());
        assertEquals(convertToTable.getColumns().size(), 1);
        Column column = convertToTable.getColumns().get(0);
        assertEquals(column.toString(),
            "Column(id=c1, name=c1, dataType=VARCHAR, length=10, comment=comment, precision=null, scale=null, primaryKey=true, nullable=true, "
                + "partitionKey=false, partitionKeyIndex=null)");
    }

    @Test
    public void convertToTableComplexDecimal() {
        List<ColumnDefinition> defines = Lists.newArrayList();
        defines.add(ColumnDefinition.builder()
            .colName(new Identifier("c1"))
            .dataType(DataTypeUtil.simpleType(DataTypeEnums.DECIMAL, new NumericParameter("10"), new NumericParameter("11")))
            .comment(new Comment("comment"))
            .primary(false)
            .notNull(true)
            .build());
        Node table = CreateTable.builder()
            .tableName(QualifiedName.of("a.b"))
            .columns(defines)
            .build();
        Table convertToTable = hologresClientConverter.convertToTable(table, HologresTransformContext.builder().build());
        assertEquals(convertToTable.getColumns().size(), 1);
        Column column = convertToTable.getColumns().get(0);
        assertEquals(
            "Column(id=c1, name=c1, dataType=DECIMAL, length=null, comment=comment, precision=10, scale=11, primaryKey=false, nullable=false, "
                + "partitionKey=false, partitionKeyIndex=null)", column.toString());
    }

    @Test
    public void convertToTablePartition() {
        List<ColumnDefinition> defines = Lists.newArrayList();
        defines.add(ColumnDefinition.builder()
            .colName(new Identifier("c1"))
            .dataType(DataTypeUtil.simpleType(DataTypeEnums.DECIMAL, new NumericParameter("10"), new NumericParameter("11")))
            .comment(new Comment("comment"))
            .primary(false)
            .notNull(true)
            .build());
        Node table = CreateTable.builder()
            .tableName(QualifiedName.of("a.b"))
            .partition(new PartitionedBy(defines))
            .build();
        Table convertToTable = hologresClientConverter.convertToTable(table, HologresTransformContext.builder().build());
        assertEquals(convertToTable.getColumns().size(), 1);
        Column column = convertToTable.getColumns().get(0);
        assertEquals(
            "Column(id=c1, name=c1, dataType=DECIMAL, length=null, comment=comment, precision=10, scale=11, primaryKey=false, nullable=false, "
                + "partitionKey=true, partitionKeyIndex=0)", column.toString());
    }

    @Test
    public void convertToTableConstraint() {
        List<ColumnDefinition> defines = Lists.newArrayList();
        defines.add(ColumnDefinition.builder()
            .colName(new Identifier("c1"))
            .dataType(DataTypeUtil.simpleType(DataTypeEnums.DECIMAL, new NumericParameter("10"), new NumericParameter("11")))
            .comment(new Comment("comment"))
            .primary(false)
            .notNull(true)
            .build());
        List<BaseConstraint> constrains = ImmutableList.of(
            new PrimaryConstraint(
                new Identifier("c1"),
                ImmutableList.of(new Identifier("c1"))
            ),
            new UniqueConstraint(new Identifier("c2"), ImmutableList.of(
                new Identifier("c1")
            ))
        );
        Node table = CreateTable.builder()
            .tableName(QualifiedName.of("a.b"))
            .partition(new PartitionedBy(defines))
            .constraints(constrains)
            .build();
        Table convertToTable = hologresClientConverter.convertToTable(table, HologresTransformContext.builder().build());
        assertEquals(convertToTable.getColumns().size(), 1);
        assertEquals(new Identifier("c1")
            , constrains.get(0).getName());
        assertEquals(new Identifier("c2")
            , constrains.get(1).getName());
    }

    @Test
    public void testPartitionWith() {
        HologresClientConverter hologresClientConverter = new HologresClientConverter();
        List<ColumnDefinition> columns = ImmutableList.of(
            ColumnDefinition.builder()
                .colName(new Identifier("c1"))
                .dataType(DataTypeUtil.simpleType(HologresDataTypeName.BIGINT))
                .primary(true)
                .build(),
            ColumnDefinition.builder()
                .colName(new Identifier("c2"))
                .dataType(DataTypeUtil.simpleType(HologresDataTypeName.BIGINT))
                .primary(true)
                .build()
        );

        List<ColumnDefinition> columns2 = ImmutableList.of(
            ColumnDefinition.builder()
                .colName(new Identifier("c1"))
                .dataType(DataTypeUtil.simpleType(HologresDataTypeName.BIGINT))
                .build()
            ,
            ColumnDefinition.builder()
                .colName(new Identifier("c3"))
                .dataType(DataTypeUtil.simpleType(HologresDataTypeName.BIGINT))
                .build()
        );
        PartitionedBy partition = new PartitionedBy(
            columns2
        );
        Node table = CreateTable.builder()
            .tableName(QualifiedName.of("abc"))
            .columns(columns)
            .partition(partition)
            .build();
        Table table1 = hologresClientConverter.convertToTable(table, HologresTransformContext.builder().build());
        assertEquals(table1.getColumns().size(), 3);
        List<Column> columns1 = table1.getColumns();
        Column column = columns1.get(2);
        assertEquals(column.getName(), "c3");
        assertTrue(column.getPartitionKeyIndex() == 1);
    }
}
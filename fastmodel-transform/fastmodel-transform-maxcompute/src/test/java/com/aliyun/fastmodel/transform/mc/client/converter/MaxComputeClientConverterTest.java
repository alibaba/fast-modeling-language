/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.mc.client.converter;

import java.util.List;

import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeParameter;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.transform.api.client.PropertyConverter;
import com.aliyun.fastmodel.transform.api.client.dto.constraint.Constraint;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.mc.client.property.LifeCycle;
import com.aliyun.fastmodel.transform.mc.parser.tree.datatype.MaxComputeDataTypeName;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

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
        Node node = maxComputeClientConverter.covertToNode(build);
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
}
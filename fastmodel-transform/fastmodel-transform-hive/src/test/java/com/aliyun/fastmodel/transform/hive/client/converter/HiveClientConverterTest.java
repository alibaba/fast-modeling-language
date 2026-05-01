/*
 * Copyright 2021-2022 Alibaba Group Holding Ltd.
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

package com.aliyun.fastmodel.transform.hive.client.converter;

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
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.client.dto.table.TableConfig;
import com.aliyun.fastmodel.transform.hive.client.property.StorageFormat;
import com.aliyun.fastmodel.transform.hive.client.property.StorageFormat.StorageFormatEnum;
import com.aliyun.fastmodel.transform.hive.context.HiveTransformContext;
import com.aliyun.fastmodel.transform.hive.format.HivePropertyKey;
import com.aliyun.fastmodel.transform.hive.parser.tree.datatype.HiveDataTypeName;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/8/5
 */
public class HiveClientConverterTest {

    HiveClientConverter hiveClientConverter = new HiveClientConverter();

    @Test
    public void getPropertyConverter() {
        PropertyConverter propertyConverter = hiveClientConverter.getPropertyConverter();
        StorageFormat baseClientProperty = (StorageFormat)propertyConverter.create(HivePropertyKey.STORAGE_FORMAT.getValue(), "orc");
        assertEquals(baseClientProperty.valueString(), StorageFormatEnum.ORC.getValue());
    }

    @Test
    public void getDataTypeZero() {
        Column column = Column.builder()
            .name("c1")
            .dataType("bigint")
            .comment("comment")
            .build();
        BaseDataType dataType = hiveClientConverter.getDataType(column);
        assertEquals(dataType.getTypeName(), DataTypeEnums.BIGINT);
    }

    @Test
    public void getDataTypeZeroChar() {
        Column column = Column.builder()
            .name("c1")
            .dataType("char")
            .length(1)
            .comment("comment")
            .build();
        BaseDataType dataType = hiveClientConverter.getDataType(column);
        assertEquals(dataType.getTypeName().getValue(), DataTypeEnums.CHAR.getValue());
    }

    @Test
    public void getDataTypeOne() {
        Column column = Column.builder()
            .name("c1")
            .dataType("varchar")
            .length(1)
            .comment("comment")
            .build();
        BaseDataType dataType = hiveClientConverter.getDataType(column);
        assertEquals(dataType.getTypeName(), HiveDataTypeName.VARCHAR);
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
        BaseDataType dataType = hiveClientConverter.getDataType(column);
        assertEquals(dataType.getTypeName(), HiveDataTypeName.DECIMAL);
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
        BaseDataType dataType = hiveClientConverter.getDataType(column);
        assertEquals(dataType.getTypeName().getName(), HiveDataTypeName.ARRAY.getName());
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
        BaseDataType dataType = hiveClientConverter.getDataType(column);
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
        Node node = hiveClientConverter.convertToNode(build, TableConfig.builder().build());
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
    public void testConvertToTable() {
        List<Property> properties = Lists.newArrayList();
        properties.add(new Property(HivePropertyKey.EXTERNAL_TABLE.getValue(), "true"));
        properties.add(new Property(HivePropertyKey.STORAGE_FORMAT.getValue(), "orc"));
        Node table = CreateTable.builder()
            .tableName(QualifiedName.of("abc"))
            .properties(properties)
            .build();
        Table table1 = hiveClientConverter.convertToTable(table, HiveTransformContext.builder().build());
        assertTrue(table1.isExternal());
        List<BaseClientProperty> properties1 = table1.getProperties();
        BaseClientProperty baseClientProperty = properties1.get(1);
        assertEquals(baseClientProperty.getKey(), HivePropertyKey.STORAGE_FORMAT.getValue());
    }

    @Test
    public void testConvertToNode() {
        List<BaseClientProperty> properties = Lists.newArrayList();
        StorageFormat e = new StorageFormat();
        e.setValue(StorageFormatEnum.ORC);
        properties.add(e);
        List<Column> columns = Lists.newArrayList();
        Column ce = Column.builder()
            .name("n1")
            .dataType("bigint")
            .build();
        columns.add(ce);
        Table table = Table.builder()
            .columns(columns)
            .external(true)
            .name("abc")
            .properties(properties)
            .build();
        Node node = hiveClientConverter.convertToNode(table, TableConfig.builder().build());
        assertEquals(node.toString(), "CREATE TABLE IF NOT EXISTS abc \n"
            + "(\n"
            + "   n1 BIGINT NOT NULL\n"
            + ")\n"
            + "WITH('hive.storage_format'='ORC','hive.table_external'='true')");
    }

    @Test
    public void testGetDataTypeUnionType() {
        Column column = Column.builder().name("c1").dataType("uniontype<int>").build();
        BaseDataType dataType = hiveClientConverter.getDataType(column);
        assertNotNull(dataType);
    }
}
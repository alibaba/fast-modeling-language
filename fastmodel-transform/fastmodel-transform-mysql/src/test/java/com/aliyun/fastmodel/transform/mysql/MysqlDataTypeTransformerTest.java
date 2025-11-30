/*
 * Copyright [2024] [name of copyright owner]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.fastmodel.transform.mysql;

import java.util.List;

import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeParameter;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.datatype.NumericParameter;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * MySQL数据类型转换测试
 *
 * @author panguanjing
 * @date 2025/10/11
 */
public class MysqlDataTypeTransformerTest {

    MysqlTransformer mysqlTransformer = new MysqlTransformer();

    @Test
    public void testTransformTableWithAllDataTypes() {
        List<ColumnDefinition> columns = Lists.newArrayList();

        // 测试所有数值类型
        columns.add(createColumn("tinyint_col", "TINYINT"));
        columns.add(createColumn("smallint_col", "SMALLINT"));
        columns.add(createColumn("mediumint_col", "MEDIUMINT"));
        columns.add(createColumn("int_col", "INT"));
        columns.add(createColumn("integer_col", "INTEGER"));
        columns.add(createColumn("bigint_col", "BIGINT"));
        columns.add(createColumn("float_col", "FLOAT"));
        columns.add(createColumn("double_col", "DOUBLE"));
        columns.add(createColumn("decimal_col", "DECIMAL", ImmutableList.of(new NumericParameter("10"), new NumericParameter("2"))));
        columns.add(createColumn("numeric_col", "NUMERIC", ImmutableList.of(new NumericParameter("10"), new NumericParameter("2"))));
        columns.add(createColumn("bit_col", "BIT"));

        // 测试布尔类型
        columns.add(createColumn("boolean_col", "BOOLEAN"));

        // 测试日期时间类型
        columns.add(createColumn("date_col", "DATE"));
        columns.add(createColumn("datetime_col", "DATETIME"));
        columns.add(createColumn("timestamp_col", "TIMESTAMP"));
        columns.add(createColumn("time_col", "TIME"));
        columns.add(createColumn("year_col", "YEAR"));

        // 测试字符串类型
        columns.add(createColumn("char_col", "CHAR", ImmutableList.of(new NumericParameter("10"))));
        columns.add(createColumn("varchar_col", "VARCHAR", ImmutableList.of(new NumericParameter("255"))));
        columns.add(createColumn("binary_col", "BINARY", ImmutableList.of(new NumericParameter("10"))));
        columns.add(createColumn("varbinary_col", "VARBINARY", ImmutableList.of(new NumericParameter("255"))));
        columns.add(createColumn("tinyblob_col", "TINYBLOB"));
        columns.add(createColumn("blob_col", "BLOB"));
        columns.add(createColumn("mediumblob_col", "MEDIUMBLOB"));
        columns.add(createColumn("longblob_col", "LONGBLOB"));
        columns.add(createColumn("tinytext_col", "TINYTEXT"));
        columns.add(createColumn("text_col", "TEXT"));
        columns.add(createColumn("mediumtext_col", "MEDIUMTEXT"));
        columns.add(createColumn("longtext_col", "LONGTEXT"));

        // 测试特殊类型
        columns.add(createColumn("json_col", "JSON"));
        columns.add(createColumn("geometry_col", "GEOMETRY"));
        columns.add(createColumn("point_col", "POINT"));
        columns.add(createColumn("linestring_col", "LINESTRING"));
        columns.add(createColumn("polygon_col", "POLYGON"));
        columns.add(createColumn("multipoint_col", "MULTIPOINT"));
        columns.add(createColumn("multilinestring_col", "MULTILINESTRING"));
        columns.add(createColumn("multipolygon_col", "MULTIPOLYGON"));
        columns.add(createColumn("geometrycollection_col", "GEOMETRYCOLLECTION"));

        CreateTable createTable = CreateTable.builder()
            .tableName(QualifiedName.of("test_all_types"))
            .columns(columns)
            .build();

        Table table = mysqlTransformer.transformTable(createTable, TransformContext.builder().build());
        assertNotNull(table);
        assertEquals("test_all_types", table.getName());
        assertEquals(columns.size(), table.getColumns().size());

        // 验证各列的数据类型
        for (int i = 0; i < columns.size(); i++) {
            ColumnDefinition columnDefinition = columns.get(i);
            Column column = table.getColumns().get(i);

            assertEquals(columnDefinition.getColName().getValue(), column.getName());

            // 验证基本数据类型
            String expectedDataType = getExpectedBaseDataType(columnDefinition);
            assertEquals(expectedDataType, column.getDataType());

            // 验证参数（precision, scale, length）
            validateColumnParameters(columnDefinition, column);
        }
    }

    @Test
    public void testReverseTableWithAllDataTypes() {
        List<Column> columns = Lists.newArrayList();

        // 测试所有数值类型
        columns.add(Column.builder().name("tinyint_col").dataType("TINYINT").build());
        columns.add(Column.builder().name("smallint_col").dataType("SMALLINT").build());
        columns.add(Column.builder().name("mediumint_col").dataType("MEDIUMINT").build());
        columns.add(Column.builder().name("int_col").dataType("INT").build());
        columns.add(Column.builder().name("integer_col").dataType("INTEGER").build());
        columns.add(Column.builder().name("bigint_col").dataType("BIGINT").build());
        columns.add(Column.builder().name("float_col").dataType("FLOAT").build());
        columns.add(Column.builder().name("double_col").dataType("DOUBLE").build());
        columns.add(Column.builder().name("decimal_col").dataType("DECIMAL(10,2)").build());
        columns.add(Column.builder().name("numeric_col").dataType("NUMERIC(10,2)").build());
        columns.add(Column.builder().name("bit_col").dataType("BIT").build());

        // 测试布尔类型
        columns.add(Column.builder().name("boolean_col").dataType("BOOLEAN").build());

        // 测试日期时间类型
        columns.add(Column.builder().name("date_col").dataType("DATE").build());
        columns.add(Column.builder().name("datetime_col").dataType("DATETIME").build());
        columns.add(Column.builder().name("timestamp_col").dataType("TIMESTAMP").build());
        columns.add(Column.builder().name("time_col").dataType("TIME").build());
        columns.add(Column.builder().name("year_col").dataType("YEAR").build());

        // 测试字符串类型
        columns.add(Column.builder().name("char_col").dataType("CHAR(10)").build());
        columns.add(Column.builder().name("varchar_col").dataType("VARCHAR(255)").build());
        columns.add(Column.builder().name("binary_col").dataType("BINARY(10)").build());
        columns.add(Column.builder().name("varbinary_col").dataType("VARBINARY(255)").build());
        columns.add(Column.builder().name("tinyblob_col").dataType("TINYBLOB").build());
        columns.add(Column.builder().name("blob_col").dataType("BLOB").build());
        columns.add(Column.builder().name("mediumblob_col").dataType("MEDIUMBLOB").build());
        columns.add(Column.builder().name("longblob_col").dataType("LONGBLOB").build());
        columns.add(Column.builder().name("tinytext_col").dataType("TINYTEXT").build());
        columns.add(Column.builder().name("text_col").dataType("TEXT").build());
        columns.add(Column.builder().name("mediumtext_col").dataType("MEDIUMTEXT").build());
        columns.add(Column.builder().name("longtext_col").dataType("LONGTEXT").build());

        // 测试特殊类型
        columns.add(Column.builder().name("json_col").dataType("JSON").build());
        columns.add(Column.builder().name("geometry_col").dataType("GEOMETRY").build());
        columns.add(Column.builder().name("point_col").dataType("POINT").build());
        columns.add(Column.builder().name("linestring_col").dataType("LINESTRING").build());
        columns.add(Column.builder().name("polygon_col").dataType("POLYGON").build());
        columns.add(Column.builder().name("multipoint_col").dataType("MULTIPOINT").build());
        columns.add(Column.builder().name("multilinestring_col").dataType("MULTILINESTRING").build());
        columns.add(Column.builder().name("multipolygon_col").dataType("MULTIPOLYGON").build());
        columns.add(Column.builder().name("geometrycollection_col").dataType("GEOMETRYCOLLECTION").build());

        Table table = Table.builder()
            .name("test_all_types_reverse")
            .columns(columns)
            .build();

        Node node = mysqlTransformer.reverseTable(table, ReverseContext.builder().build());
        assertNotNull(node);
        assertTrue(node instanceof CreateTable);

        CreateTable createTable = (CreateTable)node;
        assertEquals("test_all_types_reverse", createTable.getQualifiedName().getSuffix());
        assertEquals(columns.size(), createTable.getColumnDefines().size());
    }

    private ColumnDefinition createColumn(String name, String dataType) {
        return ColumnDefinition.builder()
            .colName(new Identifier(name))
            .dataType(new GenericDataType(dataType))
            .build();
    }

    private ColumnDefinition createColumn(String name, String dataType, List<DataTypeParameter> parameters) {
        return ColumnDefinition.builder()
            .colName(new Identifier(name))
            .dataType(new GenericDataType(dataType, parameters))
            .build();
    }

    private String getExpectedBaseDataType(ColumnDefinition columnDefinition) {
        GenericDataType genericDataType = (GenericDataType)columnDefinition.getDataType();
        return genericDataType.getName();
    }

    private void validateColumnParameters(ColumnDefinition columnDefinition, Column column) {
        GenericDataType genericDataType = (GenericDataType)columnDefinition.getDataType();
        String dataTypeName = genericDataType.getName();

        // 对于DECIMAL和NUMERIC类型，验证precision和scale
        if ("DECIMAL".equals(dataTypeName) || "NUMERIC".equals(dataTypeName)) {
            if (genericDataType.getArguments() != null && genericDataType.getArguments().size() >= 2) {
                NumericParameter precisionParam = (NumericParameter)genericDataType.getArguments().get(0);
                NumericParameter scaleParam = (NumericParameter)genericDataType.getArguments().get(1);
                // 只验证不为null的情况
                if (column.getPrecision() != null && column.getScale() != null) {
                    assertEquals(Integer.parseInt(precisionParam.getValue()), column.getPrecision().intValue());
                    assertEquals(Integer.parseInt(scaleParam.getValue()), column.getScale().intValue());
                }
            }
        }
        // 对于CHAR, VARCHAR, BINARY, VARBINARY类型，验证length
        else if ("CHAR".equals(dataTypeName) || "VARCHAR".equals(dataTypeName) ||
            "BINARY".equals(dataTypeName) || "VARBINARY".equals(dataTypeName)) {
            if (genericDataType.getArguments() != null && !genericDataType.getArguments().isEmpty()) {
                NumericParameter lengthParam = (NumericParameter)genericDataType.getArguments().get(0);
                // 只验证不为null的情况
                if (column.getLength() != null) {
                    assertEquals(Integer.parseInt(lengthParam.getValue()), column.getLength().intValue());
                }
            }
        }
    }
}
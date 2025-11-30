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

package com.aliyun.fastmodel.transform.mysql;

import java.util.List;

import com.aliyun.fastmodel.core.parser.FastModelParser;
import com.aliyun.fastmodel.core.parser.FastModelParserFactory;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.datatype.NumericParameter;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.constants.ConstraintType;
import com.aliyun.fastmodel.core.tree.statement.constants.TableDetailType;
import com.aliyun.fastmodel.core.tree.statement.table.AddCols;
import com.aliyun.fastmodel.core.tree.statement.table.AddConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropCol;
import com.aliyun.fastmodel.core.tree.statement.table.DropConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.DimConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.mysql.context.MysqlTransformContext;
import com.aliyun.fastmodel.transform.mysql.context.MysqlTransformContext.Builder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * mysql v8 转换器内容
 *
 * @author panguanjing
 * @date 2021/6/24
 */
public class MysqlTransformerTest {
    MysqlTransformer mysqlV8Transformer = new MysqlTransformer();

    CreateTable createTable1 = null;

    FastModelParser fastModelParser = FastModelParserFactory.getInstance().get();

    @Before
    public void setUp() throws Exception {
        ColumnDefinition a = ColumnDefinition.builder().colName(new Identifier("a"))
            .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
            .primary(true)
            .build();

        ColumnDefinition b = ColumnDefinition.builder().colName(new Identifier("b"))
            .dataType(DataTypeUtil.simpleType(DataTypeEnums.STRING))
            .primary(true)
            .build();
        List<ColumnDefinition> columns = ImmutableList.of(
            a, b
        );
        createTable1 = CreateTable.builder().tableName(QualifiedName.of("dim_shop")).columns(columns)
            .detailType(TableDetailType.NORMAL_DIM)
            .ifNotExist(true)
            .build();
    }

    @Test
    public void transform() {
        Builder builder = MysqlTransformContext.builder().autoIncrement(true).varcharLength(128);
        DialectNode transform = mysqlV8Transformer.transform(createTable1, builder.build());
        assertEquals("CREATE TABLE IF NOT EXISTS dim_shop\n(\n"
            + "   a BIGINT AUTO_INCREMENT  PRIMARY KEY,\n"
            + "   b VARCHAR(128) PRIMARY KEY\n"
            + ")", transform.getNode());
    }

    @Test
    public void testTransofrmNotAutoIncrement() {
        Builder builder = MysqlTransformContext.builder().autoIncrement(false).varcharLength(256);
        DialectNode transform = mysqlV8Transformer.transform(createTable1, builder.build());
        assertEquals("CREATE TABLE IF NOT EXISTS dim_shop\n(\n"
            + "   a BIGINT PRIMARY KEY,\n"
            + "   b VARCHAR(256) PRIMARY KEY\n"
            + ")", transform.getNode());
    }

    @Test
    public void testTransofrmAddCols() {
        ColumnDefinition col1 = ColumnDefinition.builder().colName(new Identifier("col1")).dataType(
            DataTypeUtil.simpleType(DataTypeEnums.BIGINT)).notNull(true).build();
        ColumnDefinition col2 = ColumnDefinition.builder().colName(new Identifier("col2")).dataType(
            DataTypeUtil.simpleType(DataTypeEnums.BIGINT)).notNull(true).build();
        AddCols addCols = new AddCols(
            QualifiedName.of("dim_shop"),
            ImmutableList.of(col1, col2)
        );
        DialectNode dialectNode = mysqlV8Transformer.transform(addCols, MysqlTransformContext.builder().build());
        assertEquals("ALTER TABLE dim_shop ADD COLUMN\n"
            + "(\n"
            + "   col1 BIGINT NOT NULL,\n"
            + "   col2 BIGINT NOT NULL\n"
            + ")", dialectNode.getNode());
    }

    @Test
    public void testTransofrmAddColsPrimary() {
        ColumnDefinition col1 = ColumnDefinition.builder().colName(new Identifier("col1")).dataType(
            DataTypeUtil.simpleType(DataTypeEnums.BIGINT)).primary(true).build();
        ColumnDefinition col2 = ColumnDefinition.builder().colName(new Identifier("col2")).dataType(
            DataTypeUtil.simpleType(DataTypeEnums.STRING)).notNull(false).build();
        AddCols addCols = new AddCols(
            QualifiedName.of("dim_shop"),
            ImmutableList.of(col1, col2)
        );
        DialectNode dialectNode = mysqlV8Transformer.transform(addCols, MysqlTransformContext.builder().build());
        assertEquals("ALTER TABLE dim_shop ADD COLUMN\n"
                + "(\n"
                + "   col1 BIGINT PRIMARY KEY,\n"
                + "   col2 VARCHAR(128) NULL\n"
                + ")",
            dialectNode.getNode());
    }

    @Test
    public void testTransformDropCols() {
        DropCol dropCol = new DropCol(QualifiedName.of("dim_shop"), new Identifier("col1"));
        DialectNode transform = mysqlV8Transformer.transform(dropCol, MysqlTransformContext.builder().build());
        assertEquals(transform.getNode(), "ALTER TABLE dim_shop DROP COLUMN col1");
    }

    @Test
    public void testTransformAddConstraint() {
        AddConstraint addConstraint = new AddConstraint(QualifiedName.of("dim_shop"), new PrimaryConstraint(
            new Identifier("c1"),
            ImmutableList.of(new Identifier("c1"))
        ));
        DialectNode dialectNode = mysqlV8Transformer.transform(addConstraint, MysqlTransformContext.builder().build());
        assertEquals(dialectNode.getNode(), "ALTER TABLE dim_shop ADD CONSTRAINT c1 PRIMARY KEY (c1)");
    }

    @Test
    public void testTransformDropPrimaryConstraint() {
        DropConstraint dropConstraint = new DropConstraint(QualifiedName.of("abc"), new Identifier("c1"),
            ConstraintType.PRIMARY_KEY);
        DialectNode dialectNode = mysqlV8Transformer.transform(dropConstraint, MysqlTransformContext.builder().build());
        assertEquals(dialectNode.getNode(), "ALTER TABLE abc DROP PRIMARY KEY");
    }

    @Test
    public void testTransformDropNotNullConstraint() {
        DropConstraint dropConstraint = new DropConstraint(QualifiedName.of("abc"), new Identifier("c1"),
            ConstraintType.NOT_NULL);
        DialectNode dialectNode = mysqlV8Transformer.transform(dropConstraint, MysqlTransformContext.builder().build());
        assertEquals(dialectNode.getNode(), "ALTER TABLE abc DROP CONSTRAINT c1");
        assertFalse(dialectNode.isExecutable());
    }

    @Test
    public void testTransformDropForigenConstraint() {
        DropConstraint dropConstraint = new DropConstraint(QualifiedName.of("abc"), new Identifier("c1"),
            ConstraintType.DIM_KEY);
        DialectNode dialectNode = mysqlV8Transformer.transform(dropConstraint,
            MysqlTransformContext.builder().generateForeignKey(true).build());
        assertEquals(dialectNode.getNode(), "ALTER TABLE abc DROP FOREIGN KEY c1");
        assertTrue(dialectNode.isExecutable());
    }

    @Test
    public void testTransformAddForigenKey() {
        AddConstraint addConstraint = new AddConstraint(QualifiedName.of("dim_shop"), new DimConstraint(
            new Identifier("c1"),
            ImmutableList.of(new Identifier("c2")),
            QualifiedName.of("refTable"),
            ImmutableList.of(new Identifier("c2"))
        ));
        DialectNode transform = mysqlV8Transformer.transform(addConstraint,
            MysqlTransformContext.builder().generateForeignKey(true).build());
        String node = transform.getNode();
        assertEquals("ALTER TABLE dim_shop ADD CONSTRAINT c1 FOREIGN KEY (c2) REFERENCES refTable(c2)", node);
    }

    @Test
    public void testTransform() {
        String fml = "REF a.id -> b.id : name";
        BaseStatement baseStatement = fastModelParser.parseStatement(fml);
        DialectNode transform = mysqlV8Transformer.transform(baseStatement);
        assertEquals(transform.getNode(), "ALTER TABLE a ADD CONSTRAINT name FOREIGN KEY (id) REFERENCES b (id)");
    }

    @Test
    public void testTransformTable() {
        // 创建一个带有各种数据类型的建表语句
        List<ColumnDefinition> columns = Lists.newArrayList();

        // BIGINT列
        ColumnDefinition idCol = ColumnDefinition.builder()
            .colName(new Identifier("id"))
            .dataType(new GenericDataType(new Identifier("BIGINT")))
            .comment(new Comment("主键ID"))
            .primary(true)
            .build();
        columns.add(idCol);

        // VARCHAR列
        ColumnDefinition nameCol = ColumnDefinition.builder()
            .colName(new Identifier("name"))
            .dataType(new GenericDataType(new Identifier("VARCHAR"), ImmutableList.of(new NumericParameter("255"))))
            .comment(new Comment("名称"))
            .build();
        columns.add(nameCol);

        // DECIMAL列
        ColumnDefinition priceCol = ColumnDefinition.builder()
            .colName(new Identifier("price"))
            .dataType(new GenericDataType(new Identifier("DECIMAL"), ImmutableList.of(new NumericParameter("10"), new NumericParameter("2"))))
            .comment(new Comment("价格"))
            .build();
        columns.add(priceCol);

        CreateTable createTable = CreateTable.builder()
            .tableName(QualifiedName.of("product"))
            .columns(columns)
            .comment(new Comment("产品表"))
            .build();

        Table table = mysqlV8Transformer.transformTable(createTable, TransformContext.builder().build());
        assertNotNull(table);
        assertEquals("product", table.getName());
        assertEquals(3, table.getColumns().size());

        // 验证各列的数据类型
        Column idColumn = table.getColumns().get(0);
        assertEquals("id", idColumn.getName());
        assertEquals("BIGINT", idColumn.getDataType());
        // 注意：Column类中没有isPrimary方法，所以我们不测试这个

        Column nameColumn = table.getColumns().get(1);
        assertEquals("name", nameColumn.getName());
        assertEquals("VARCHAR", nameColumn.getDataType());

        Column priceColumn = table.getColumns().get(2);
        assertEquals("price", priceColumn.getName());
        assertEquals("DECIMAL", priceColumn.getDataType());
    }

    @Test
    public void testReverseTable() {
        // 测试基本数据类型
        List<Column> columns = Lists.newArrayList();

        // 测试BIGINT类型
        Column bigintCol = Column.builder()
            .name("id")
            .dataType("BIGINT")
            .comment("主键ID")
            .build();
        columns.add(bigintCol);

        // 测试VARCHAR类型
        Column varcharCol = Column.builder()
            .name("name")
            .dataType("VARCHAR(255)")
            .comment("名称")
            .build();
        columns.add(varcharCol);

        // 测试DECIMAL类型
        Column decimalCol = Column.builder()
            .name("price")
            .dataType("DECIMAL(10,2)")
            .comment("价格")
            .build();
        columns.add(decimalCol);

        // 测试TEXT类型
        Column textCol = Column.builder()
            .name("description")
            .dataType("TEXT")
            .comment("描述")
            .build();
        columns.add(textCol);

        // 测试DATETIME类型
        Column datetimeCol = Column.builder()
            .name("created_time")
            .dataType("DATETIME")
            .comment("创建时间")
            .build();
        columns.add(datetimeCol);

        Table table = Table.builder()
            .name("test_table")
            .columns(columns)
            .comment("测试表")
            .build();

        Node node = mysqlV8Transformer.reverseTable(table, ReverseContext.builder().build());
        assertNotNull(node);
        assertTrue(node instanceof CreateTable);

        CreateTable createTable = (CreateTable)node;
        assertEquals("test_table", createTable.getQualifiedName().getSuffix());
        assertEquals(5, createTable.getColumnDefines().size());
    }

    @Test
    public void testReverseTableWithMultipleDataTypes() {
        // 测试更多MySQL 8.0数据类型
        List<Column> columns = Lists.newArrayList();

        // TINYINT列
        Column tinyIntCol = Column.builder()
            .name("status")
            .dataType("TINYINT")
            .comment("状态")
            .build();
        columns.add(tinyIntCol);

        // TEXT列
        Column textCol = Column.builder()
            .name("content")
            .dataType("TEXT")
            .comment("内容")
            .build();
        columns.add(textCol);

        // DATETIME列
        Column datetimeCol = Column.builder()
            .name("created_at")
            .dataType("DATETIME")
            .comment("创建时间")
            .build();
        columns.add(datetimeCol);

        Table table = Table.builder()
            .name("article")
            .columns(columns)
            .build();

        Node node = mysqlV8Transformer.reverseTable(table, ReverseContext.builder().build());
        assertNotNull(node);
        assertTrue(node instanceof CreateTable);

        CreateTable createTable = (CreateTable)node;
        assertEquals("article", createTable.getQualifiedName().getSuffix());
        assertEquals(3, createTable.getColumnDefines().size());
    }

    @Test
    public void testReverseTableFromTableTxt() {
        // 基于table.txt中的表结构创建测试
        List<Column> columns = Lists.newArrayList();

        // 根据table.txt文件中的表结构定义创建列
        Column idCol = Column.builder()
            .name("id")
            .dataType("BIGINT")
            .length(38) // 根据table.txt: length=38
            .precision(38) // 根据table.txt: precision=38
            .scale(18) // 根据table.txt: scale=18
            .nullable(false)  // 根据table.txt显示，此列非空
            .build();
        columns.add(idCol);

        Column fDescCol = Column.builder()
            .name("f_desc")
            .dataType("LONGTEXT")
            .length(38) // 根据table.txt: length=38
            .precision(38) // 根据table.txt: precision=38
            .scale(18) // 根据table.txt: scale=18
            .nullable(true)
            .build();
        columns.add(fDescCol);

        Column fSmallIntCol = Column.builder()
            .name("f_smallint")
            .dataType("SMALLINT")
            .length(38) // 根据table.txt: length=38
            .precision(38) // 根据table.txt: precision=38
            .scale(18) // 根据table.txt: scale=18
            .nullable(true)
            .build();
        columns.add(fSmallIntCol);

        Column fIntegerCol = Column.builder()
            .name("f_integer")
            .dataType("INT")
            .length(38) // 根据table.txt: length=38
            .precision(38) // 根据table.txt: precision=38
            .scale(18) // 根据table.txt: scale=18
            .nullable(true)
            .build();
        columns.add(fIntegerCol);

        Column fBigintCol = Column.builder()
            .name("f_bigint")
            .dataType("BIGINT")
            .length(38) // 根据table.txt: length=38
            .precision(38) // 根据table.txt: precision=38
            .scale(18) // 根据table.txt: scale=18
            .nullable(true)
            .build();
        columns.add(fBigintCol);

        // DECIMAL类型的列 - 有不同精度和小数位
        Column fDecimal38_18 = Column.builder()
            .name("f_decimal_38_18")
            .dataType("DECIMAL")
            .length(30) // 根据table.txt: length=30
            .precision(30) // 根据table.txt: precision=30
            .scale(18) // 根据table.txt: scale=18
            .nullable(true)
            .build();
        columns.add(fDecimal38_18);

        Column fDecimal38_0 = Column.builder()
            .name("f_decimal_38_0")
            .dataType("DECIMAL")
            .length(30) // 根据table.txt: length=30
            .precision(30) // 根据table.txt: precision=30
            .scale(18) // 根据table.txt: scale=18
            .nullable(true)
            .build();
        columns.add(fDecimal38_0);

        Column fDecimal1_0 = Column.builder()
            .name("f_decimal_1_0")
            .dataType("DECIMAL")
            .length(30) // 根据table.txt: length=30
            .precision(30) // 根据table.txt: precision=30
            .scale(18) // 根据table.txt: scale=18
            .nullable(true)
            .build();
        columns.add(fDecimal1_0);

        Column fDecimal1_1 = Column.builder()
            .name("f_decimal_1_1")
            .dataType("DECIMAL")
            .length(30) // 根据table.txt: length=30
            .precision(30) // 根据table.txt: precision=30
            .scale(18) // 根据table.txt: scale=18
            .nullable(true)
            .build();
        columns.add(fDecimal1_1);

        Column fRealCol = Column.builder()
            .name("f_real")
            .dataType("FLOAT")
            .length(38) // 根据table.txt: length=38
            .precision(38) // 根据table.txt: precision=38
            .scale(18) // 根据table.txt: scale=18
            .nullable(true)
            .build();
        columns.add(fRealCol);

        Column fDoubleCol = Column.builder()
            .name("f_double")
            .dataType("DOUBLE")
            .length(38) // 根据table.txt: length=38
            .precision(38) // 根据table.txt: precision=38
            .scale(18) // 根据table.txt: scale=18
            .nullable(true)
            .build();
        columns.add(fDoubleCol);

        Column fBooleanCol = Column.builder()
            .name("f_boolean")
            .dataType("TINYINT")
            .length(38) // 根据table.txt: length=38
            .precision(38) // 根据table.txt: precision=38
            .scale(18) // 根据table.txt: scale=18
            .nullable(true)
            .build();
        columns.add(fBooleanCol);

        Column fVarchar10Col = Column.builder()
            .name("f_serial_integer")
            .dataType("VARCHAR")
            .length(10) // 根据table.txt: length=10
            .precision(10) // 根据table.txt: precision=10
            .scale(0) // 根据table.txt: scale=0
            .nullable(false)
            .build();
        columns.add(fVarchar10Col);

        Column fVarchar19Col = Column.builder()
            .name("f_serial_bigint")
            .dataType("VARCHAR")
            .length(19) // 根据table.txt: length=19
            .precision(19) // 根据table.txt: precision=19
            .scale(0) // 根据table.txt: scale=0
            .nullable(false)
            .build();
        columns.add(fVarchar19Col);

        Column fChar1Col = Column.builder()
            .name("f_char_1")
            .dataType("CHAR")
            .length(1) // 根据table.txt: length=1
            .precision(1) // 根据table.txt: precision=1
            .scale(0) // 根据table.txt: scale=0
            .nullable(true)
            .build();
        columns.add(fChar1Col);

        Column fVarchar255Col = Column.builder()
            .name("f_varchar_1")
            .dataType("VARCHAR")
            .length(255) // 根据table.txt: length=255
            .precision(255) // 根据table.txt: precision=255
            .scale(0) // 根据table.txt: scale=0
            .nullable(true)
            .build();
        columns.add(fVarchar255Col);

        Column fDateCol = Column.builder()
            .name("f_date")
            .dataType("DATE")
            .length(38) // 根据table.txt: length=38
            .precision(38) // 根据table.txt: precision=38
            .scale(18) // 根据table.txt: scale=18
            .nullable(true)
            .build();
        columns.add(fDateCol);

        Column fTimestampCol = Column.builder()
            .name("f_timestamptz")
            .dataType("TIMESTAMP")
            .length(6) // 根据table.txt: length=6
            .precision(6) // 根据table.txt: precision=6
            .scale(6) // 根据table.txt: scale=6
            .nullable(true)
            .build();
        columns.add(fTimestampCol);

        // 创建Table对象，模拟table.txt文件中的Table定义
        Table table = Table.builder()
            .name("dw_holo_db_01_dw_test_holo_src_all_type_tbl_normal")
            .database("dw_test")
            .ifNotExist(true)  // 根据table.txt显示 ifNotExist=true
            .columns(columns)
            .build();

        Node node = mysqlV8Transformer.reverseTable(table, ReverseContext.builder().build());
        assertNotNull(node);
        assertTrue(node instanceof CreateTable);

        CreateTable createTable = (CreateTable)node;
        assertEquals("dw_holo_db_01_dw_test_holo_src_all_type_tbl_normal", createTable.getQualifiedName().getSuffix());
        assertEquals(18, createTable.getColumnDefines().size());  // 验证列的数量
    }
}
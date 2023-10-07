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
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
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
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.mysql.context.MysqlTransformContext;
import com.aliyun.fastmodel.transform.mysql.context.MysqlTransformContext.Builder;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
        assertEquals("ALTER TABLE dim_shop ADD\n"
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
        assertEquals("ALTER TABLE dim_shop ADD\n"
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
        assertEquals("ALTER TABLE dim_shop ADD CONSTRAINT c1 FOREIGN KEY (c2) REFERENCES reftable(c2)", node);
    }

    @Test
    public void testTransform() {
        String fml = "REF a.id -> b.id : name";
        BaseStatement baseStatement = fastModelParser.parseStatement(fml);
        DialectNode transform = mysqlV8Transformer.transform(baseStatement);
        assertEquals(transform.getNode(), "ALTER TABLE a ADD CONSTRAINT name FOREIGN KEY (id) REFERENCES b (id)");
    }
}
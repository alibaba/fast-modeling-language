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

package com.aliyun.fastmodel.transform.mc.format;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AliasedName;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.atom.TableOrColumn;
import com.aliyun.fastmodel.core.tree.expr.enums.VarType;
import com.aliyun.fastmodel.core.tree.statement.table.AddCols;
import com.aliyun.fastmodel.core.tree.statement.table.ChangeCol;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDimTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropCol;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.mc.context.MaxComputeContext;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/11/19
 */
public class MaxComputeFormatterTest {

    @Test
    public void testTableOrColumn() {
        TableOrColumn tableOrColumn = new TableOrColumn(QualifiedName.of("abc"), VarType.MACRO);
        String s = MaxComputeFormatter.formatSql(tableOrColumn, MaxComputeContext.builder().build()).getNode();
        assertEquals("#abc#", s);
    }

    @Test
    public void testDropCol() {
        DropCol dropCol = new DropCol(
            QualifiedName.of("a.b"),
            new Identifier("col")
        );
        String s = MaxComputeFormatter.formatSql(dropCol, MaxComputeContext.builder().build()).getNode();
        assertEquals("ALTER TABLE b DROP COLUMN col", s);
    }

    @Test
    public void testChangeCol() {
        ChangeCol changeCol = new ChangeCol(
            QualifiedName.of("a.b"),
            new Identifier("col1"),
            ColumnDefinition.builder().colName(new Identifier("col1")).dataType(
                DataTypeUtil.simpleType(DataTypeEnums.DATE)
            ).build()
        );
        DialectNode dialectNode = MaxComputeFormatter.formatSql(changeCol,
            MaxComputeContext.builder().useV1DataType(true).build());
        assertEquals("ALTER TABLE b CHANGE COLUMN col1 col1 DATETIME", dialectNode.getNode());
    }

    @Test
    public void testAddCols() {
        AddCols addCols = new AddCols(
            QualifiedName.of("a.b"),
            ImmutableList.of(
                ColumnDefinition.builder().colName(
                    new Identifier("co1")
                ).dataType(DataTypeUtil.simpleType(DataTypeEnums.DATE)).build()
            )
        );
        DialectNode dialectNode = MaxComputeFormatter.formatSql(addCols, MaxComputeContext.builder().
            useV1DataType(true).build());
        assertEquals("ALTER TABLE b ADD COLUMNS\n"
            + "(\n"
            + "   co1 DATETIME\n"
            + ")", dialectNode.getNode());
    }

    @Test
    public void testCreateTable() {
        CreateDimTable createTable = CreateDimTable.builder().tableName(
            QualifiedName.of("a.b")
        ).build();
        MaxComputeContext context = MaxComputeContext.builder().appendSemicolon(true).build();
        String s = MaxComputeFormatter.formatSql(createTable, context).getNode();
        assertEquals("CREATE TABLE b;", s);
    }

    @Test
    public void testCreateTableWithNewFormat() {
        List<ColumnDefinition> columnDefinition = ImmutableList.of(
            ColumnDefinition.builder().colName(new Identifier("cabcdesd"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.STRING))
                .comment(new Comment("abc"))
                .build(),

            ColumnDefinition.builder().colName(new Identifier("cabcddddddesd"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
                .comment(new Comment("abc"))
                .build()
        );

        List<ColumnDefinition> par = ImmutableList.of(
            ColumnDefinition.builder().colName(new Identifier("xxxxsdfdfcdesd"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.STRING))
                .comment(new Comment("abc"))
                .build(),

            ColumnDefinition.builder().colName(new Identifier("dsdfsdfsdfsdfsdfsdfsdfsd"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
                .comment(new Comment("abc"))
                .build()
        );

        PartitionedBy partitionedBy = new PartitionedBy(
            par
        );

        CreateDimTable createTable = CreateDimTable.builder()
            .tableName(QualifiedName.of("a.b"))
            .columns(columnDefinition)
            .partition(partitionedBy)
            .build();

        DialectNode dialectNode = MaxComputeFormatter.formatSql(createTable, MaxComputeContext.builder().build());
        assertEquals(dialectNode.getNode(), "CREATE TABLE b\n"
            + "(\n"
            + "   cabcdesd      STRING COMMENT 'abc',\n"
            + "   cabcddddddesd BIGINT COMMENT 'abc'\n"
            + ")\n"
            + "PARTITIONED BY\n"
            + "(\n"
            + "   xxxxsdfdfcdesd           STRING COMMENT 'abc',\n"
            + "   dsdfsdfsdfsdfsdfsdfsdfsd BIGINT COMMENT 'abc'\n"
            + ")\n"
            + "");
    }

    @Test
    public void testCreateTableWithPartitions() {
        List<ColumnDefinition> columnDefinition = ImmutableList.of(ColumnDefinition.builder().colName(
                new Identifier("c1")).dataType(
                DataTypeUtil.simpleType(DataTypeEnums.VARCHAR)).comment(new Comment("abc"))
            .build());
        List<ColumnDefinition> columnDefinition2 = ImmutableList.of(ColumnDefinition.builder().colName(
                new Identifier("c2")).dataType(DataTypeUtil.simpleType(DataTypeEnums.VARCHAR)).comment(new Comment(
                "abc"))
            .build());
        CreateDimTable createTable = CreateDimTable.builder().tableName(
            QualifiedName.of("a.b")).columns(columnDefinition2).comment(
            new Comment("comment")
        ).partition(
            new PartitionedBy(columnDefinition)
        ).build();
        MaxComputeContext context = MaxComputeContext.builder().appendSemicolon(true).build();
        String s = MaxComputeFormatter.formatSql(createTable, context).getNode();
        assertEquals(s, "CREATE TABLE b\n"
            + "(\n"
            + "   c2 VARCHAR COMMENT 'abc'\n"
            + ")\n"
            + "COMMENT 'comment'\n"
            + "PARTITIONED BY\n"
            + "(\n"
            + "   c1 VARCHAR COMMENT 'abc'\n"
            + ")\n"
            + ";");
        context.setAppendSemicolon(false);
        s = MaxComputeFormatter.formatSql(createTable, context).getNode();
        assertEquals(s, "CREATE TABLE b\n"
            + "(\n"
            + "   c2 VARCHAR COMMENT 'abc'\n"
            + ")\n"
            + "COMMENT 'comment'\n"
            + "PARTITIONED BY\n"
            + "(\n"
            + "   c1 VARCHAR COMMENT 'abc'\n"
            + ")\n"
        );
    }

    @Test
    public void testFormateAliasedName() {
        CreateDimTable createDimTable = CreateDimTable.builder().tableName(QualifiedName.of("a.b")).aliasedName(
            new AliasedName("table_alias")).build();
        DialectNode dialectNode = MaxComputeFormatter.formatSql(createDimTable, MaxComputeContext.builder().build());
        assertEquals(dialectNode.getNode(), "CREATE TABLE b");
        createDimTable = CreateDimTable.builder().tableName(QualifiedName.of("a.b")).aliasedName(
            new AliasedName("table_alias")).comment(new Comment("table_comment")).build();
        dialectNode = MaxComputeFormatter.formatSql(createDimTable, MaxComputeContext.builder().build());
        assertEquals(dialectNode.getNode(), "CREATE TABLE b COMMENT 'table_comment'");
    }
}
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

package com.aliyun.fastmodel.transform.hive.format;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AliasedName;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDimTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropCol;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableComment;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.hive.context.HiveTransformContext;
import com.aliyun.fastmodel.transform.hive.context.RowFormat;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/2/1
 */
public class HiveFormatterTest {

    @Test
    public void testFormat() {
        List<Property> properties = ImmutableList.of(new Property("a", "b"), new Property("a1", "b1"));
        ImmutableList<ColumnDefinition> of = ImmutableList.of(
            ColumnDefinition.builder().colName(new Identifier("c1"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.STRING)).comment(new Comment("abc"))
                .build()
        );
        ImmutableList<ColumnDefinition> of2 = ImmutableList.of(
            ColumnDefinition.builder().colName(
                    new Identifier("c2")).dataType(DataTypeUtil.simpleType(DataTypeEnums.STRING))
                .comment(
                    new Comment("abc")
                ).build()
        );
        PartitionedBy partitionedBy = new PartitionedBy(
            of
        );
        CreateTable createTable = CreateDimTable.builder().tableName(
            QualifiedName.of("a.b")).ifNotExist(true).columns(
            of2

        ).comment(
            new Comment("comment")
        ).properties(properties).partition(
            partitionedBy
        ).build();
        HiveTransformContext context = HiveTransformContext.builder()
            .fileFormat("JSON").location("/home/admin").build();
        String format = HiveFormatter.format(createTable, context).getNode();
        assertEquals(format, "CREATE TABLE IF NOT EXISTS b\n"
            + "(\n"
            + "   c2 STRING COMMENT 'abc'\n"
            + ")\n"
            + "COMMENT 'comment'\n"
            + "PARTITIONED BY\n"
            + "(\n"
            + "   c1 STRING COMMENT 'abc'\n"
            + ")\n"
            + "STORED AS JSON\n"
            + "LOCATION '/home/admin'\n"
            + "TBLPROPERTIES ('a'='b','a1'='b1')");
        context.setRowFormat(new RowFormat(
            ",", null, null, null, null, "-"
        ));
        format = HiveFormatter.format(createTable, context).getNode();
        assertEquals("CREATE TABLE IF NOT EXISTS b\n"
            + "(\n"
            + "   c2 STRING COMMENT 'abc'\n"
            + ")\n"
            + "COMMENT 'comment'\n"
            + "PARTITIONED BY\n"
            + "(\n"
            + "   c1 STRING COMMENT 'abc'\n"
            + ")\n"
            + "DELIMITED FIELDS TERMINATED BY ',' NULL DEFINED AS '-'\n"
            + "STORED AS JSON\n"
            + "LOCATION '/home/admin'\n"
            + "TBLPROPERTIES ('a'='b','a1'='b1')", format);
    }

    @Test
    public void testDropCol() {
        DialectNode col11 = HiveFormatter.format(new DropCol(QualifiedName.of("a.b"),
            new Identifier("col1")), HiveTransformContext.builder().build());
        String col1 = col11.getNode();
        assertEquals("ALTER TABLE b DROP COLUMN col1", col1);
        assertFalse(col11.isExecutable());
    }

    @Test
    public void testTableComment() {
        SetTableComment setTableComment = new SetTableComment(
            QualifiedName.of("a.b"),
            new Comment("comment")
        );
        String format = HiveFormatter.format(setTableComment, HiveTransformContext.builder().build()
        ).getNode();
        assertEquals(format, "ALTER TABLE b SET TBLPROPERTIES('comment'='comment')");
    }

    @Test
    public void testConvert() {
        CreateDimTable createDimTable = CreateDimTable.builder().tableName(QualifiedName.of("a.b")).columns(
            ImmutableList.of(ColumnDefinition.builder().dataType(DataTypeUtil.simpleType(
                DataTypeEnums.DATETIME
            )).colName(new Identifier("col1")).build())
        ).build();
        DialectNode format = HiveFormatter.format(createDimTable, HiveTransformContext.builder().build());
        assertEquals(format.getNode(), "CREATE TABLE b\n"
            + "(\n"
            + "   col1 TIMESTAMP\n"
            + ")");
    }

    @Test
    public void testFormateAliasedName() {
        CreateDimTable createDimTable = CreateDimTable.builder().tableName(QualifiedName.of("a.b")).aliasedName(
            new AliasedName("table_alias")).build();
        DialectNode dialectNode = HiveFormatter.format(createDimTable, HiveTransformContext.builder().build());
        assertEquals(dialectNode.getNode(), "CREATE TABLE b COMMENT 'table_alias'");
        createDimTable = CreateDimTable.builder().tableName(QualifiedName.of("a.b")).aliasedName(
            new AliasedName("table_alias")).comment(new Comment("table_comment")).build();
        dialectNode = HiveFormatter.format(createDimTable, HiveTransformContext.builder().build());
        assertEquals(dialectNode.getNode(), "CREATE TABLE b COMMENT 'table_comment'");
    }
}
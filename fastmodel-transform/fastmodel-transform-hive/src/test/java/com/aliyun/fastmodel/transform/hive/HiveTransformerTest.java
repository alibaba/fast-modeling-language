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

package com.aliyun.fastmodel.transform.hive;

import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.AddCols;
import com.aliyun.fastmodel.core.tree.statement.table.AddConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.ChangeCol;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDimTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.RenameTable;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableComment;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.UnSetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.hive.context.HiveTransformContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/1/29
 */
public class HiveTransformerTest {

    HiveTransformer hiveTransformer = new HiveTransformer();

    @Test
    public void transform() {
        DialectNode transform = hiveTransformer.transform(
            CreateDimTable.builder().tableName(QualifiedName.of("a.b")).build());
        assertFalse(transform.isExecutable());
    }

    @Test
    public void transformPartition() {
        DialectNode transform = hiveTransformer.transform(
            CreateDimTable.builder().tableName(QualifiedName.of("a.b"))
                .partition(new PartitionedBy(
                    ImmutableList.of(
                        ColumnDefinition.builder()
                            .colName(new Identifier("c1"))
                            .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
                            .build()
                    )
                ))
                .build());
        assertFalse(transform.isExecutable());
        System.out.println(transform.getNode());
        assertEquals("CREATE TABLE b\n"
            + "PARTITIONED BY\n"
            + "(\n"
            + "   c1 BIGINT\n"
            + ")", transform.getNode());

    }

    @Test
    public void testTransformWithConstraint() {
        List<ColumnDefinition> columnDefines = ImmutableList.of(ColumnDefinition.builder().colName(
            new Identifier("c1")).dataType(new GenericDataType(new Identifier(DataTypeEnums.DOUBLE.name()))
        ).build());
        List<BaseConstraint> constraints = ImmutableList.of(new PrimaryConstraint(
            new Identifier("abc"),
            Lists.newArrayList(new Identifier("c1"))
        ));
        List<Property> properties = ImmutableList.of(new Property("abc", "value1"));
        HiveTransformContext context = HiveTransformContext.builder().enableConstraint(true).build();
        DialectNode abc = hiveTransformer.transform(CreateDimTable.builder().tableName(QualifiedName.of("a.b"))
                .ifNotExist(
                    true
                ).columns(columnDefines).constraints(constraints).comment(new Comment("abc")).properties(properties)
                .build(),
            context);
        assertEquals(abc.getNode(), "CREATE TABLE IF NOT EXISTS b\n"
            + "(\n"
            + "   c1 DOUBLE,\n"
            + "   CONSTRAINT abc PRIMARY KEY(c1)\n"
            + ")\n"
            + "COMMENT 'abc'\n"
            + "TBLPROPERTIES ('abc'='value1')");

        context.setEnableConstraint(false);
        abc = hiveTransformer.transform(CreateDimTable.builder().tableName(QualifiedName.of("a.b")).ifNotExist(
                true
            ).columns(columnDefines).constraints(constraints).comment(
                new Comment("abc")
            ).properties(properties).build(),
            context);
        assertEquals(abc.getNode(), "CREATE TABLE IF NOT EXISTS b\n"
            + "(\n"
            + "   c1 DOUBLE\n"
            + ")\n"
            + "COMMENT 'abc'\n"
            + "TBLPROPERTIES ('abc'='value1')");
    }

    @Test
    public void testSetComment() {
        SetTableComment setTableComment = new SetTableComment(
            QualifiedName.of("a.b"),
            new Comment("comment")
        );
        DialectNode transform = hiveTransformer.transform(setTableComment);
        String node = transform.getNode();
        assertEquals("ALTER TABLE b SET TBLPROPERTIES('comment'='comment')", node);
    }

    @Test
    public void testRename() {
        RenameTable renameTable = new RenameTable(
            QualifiedName.of("a.b"),
            QualifiedName.of("c.d")
        );
        DialectNode dialectNode = hiveTransformer.transform(renameTable);
        assertEquals(
            "ALTER TABLE b RENAME TO d",
            dialectNode.getNode()
        );
    }

    @Test
    public void testAddCols() {
        List<ColumnDefinition> columnLists =
            ImmutableList.of(ColumnDefinition.builder().colName(new Identifier("col1"))
                .dataType(new GenericDataType(new Identifier(DataTypeEnums.BIGINT.name()))).build());
        AddCols addCols = new AddCols(
            QualifiedName.of("a.b"),
            columnLists
        );
        DialectNode dialectNode = hiveTransformer.transform(addCols);
        assertEquals(dialectNode.getNode(),
            "ALTER TABLE b ADD COLUMNS\n"
                + "(\n"
                + "   col1 BIGINT\n"
                + ")");
    }

    @Test
    public void testSetProperties() {
        SetTableProperties setTableProperties = new SetTableProperties(
            QualifiedName.of("a.b"),
            ImmutableList.of(
                new Property("key", "value")
            )
        );
        DialectNode dialectNode = hiveTransformer.transform(setTableProperties);
        assertEquals(dialectNode.getNode(),
            "ALTER TABLE b SET TBLPROPERTIES('key'='value')");
    }

    @Test
    public void testUnsetProperties() {
        UnSetTableProperties unSetTableProperties = new UnSetTableProperties(
            QualifiedName.of("a.b"),
            ImmutableList.of("a", "b", "c")
        );
        DialectNode dialectNode = hiveTransformer.transform(unSetTableProperties);
        assertEquals(dialectNode.getNode(), "ALTER TABLE b UNSET TBLPROPERTIES IF EXISTS('a','b','c')");
    }

    @Test
    public void testDropTable() {
        DropTable dropTable = new DropTable(
            QualifiedName.of("a.b")
        );
        DialectNode transform = hiveTransformer.transform(dropTable);
        assertEquals(transform.getNode(), "DROP TABLE b");
    }

    @Test
    public void testChangeColumn() {
        ChangeCol changeCol = new ChangeCol(
            QualifiedName.of("a.b"),
            new Identifier("old"),
            ColumnDefinition.builder().colName(new Identifier("new_col")).dataType(new GenericDataType(
                new Identifier(DataTypeEnums.BIGINT.name())
            )).comment(new Comment("abc")).build()
        );
        DialectNode transform = hiveTransformer.transform(changeCol);
        assertEquals(transform.getNode(), "ALTER TABLE b CHANGE COLUMN old new_col BIGINT COMMENT 'abc'");
    }

    @Test
    public void testAddConstraint() {
        AddConstraint addConstraint = new AddConstraint(
            QualifiedName.of("a.b"),
            new PrimaryConstraint(new Identifier("b"), Lists.newArrayList(new Identifier("c")))
        );

        DialectNode transform = hiveTransformer.transform(addConstraint);
        assertFalse(transform.isExecutable());
        TransformContext context = HiveTransformContext.builder().enableConstraint(true).build();
        DialectNode transform1 = hiveTransformer.transform(addConstraint, context);
        assertTrue(transform1.isExecutable());
    }

    @Test
    public void testReverse() {
        BaseStatement reverse = hiveTransformer.reverse(new DialectNode("create table a(c bigint);"));
        assertEquals(reverse.toString(), "CREATE DIM TABLE a \n"
            + "(\n"
            + "   c BIGINT\n"
            + ")");
    }

    @Test
    public void testReverseWithProperties() {
        String node = "create table a(c bigint) TBLPROPERTIES('life_cycle'='10');";
        BaseStatement reverse = hiveTransformer.reverse(new DialectNode(node));
        assertEquals(reverse.toString(), "CREATE DIM TABLE a \n"
            + "(\n"
            + "   c BIGINT\n"
            + ")\nWITH('life_cycle'='10')");
    }

    @Test(expected = NullPointerException.class)
    public void testSourceIsNull() {
        hiveTransformer.transform(null);
    }

}
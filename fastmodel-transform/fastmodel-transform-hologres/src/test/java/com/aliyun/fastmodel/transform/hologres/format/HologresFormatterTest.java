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

package com.aliyun.fastmodel.transform.hologres.format;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AliasedName;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.AddCols;
import com.aliyun.fastmodel.core.tree.statement.table.AddPartitionCol;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDimTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropPartitionCol;
import com.aliyun.fastmodel.core.tree.statement.table.DropTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.RenameTable;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.hologres.context.HologresTransformContext;
import com.aliyun.fastmodel.transform.hologres.dialect.HologresVersion;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/4/15
 */
public class HologresFormatterTest {

    @Test
    public void format() {
        CreateDimTable createDimTable = CreateDimTable.builder()
            .tableName(QualifiedName.of("a"))
            .columns(
                ImmutableList.of(
                    ColumnDefinition.builder().colName(new Identifier("a")).dataType(
                        DataTypeUtil.simpleType(DataTypeEnums.DATETIME)
                    ).comment(new Comment("hello")).build(),
                    ColumnDefinition.builder().colName(new Identifier("b")).dataType(
                        DataTypeUtil.simpleType(DataTypeEnums.STRING)
                    ).primary(true).build()
                )
            ).comment(new Comment("comment")).build();
        DialectNode format = HologresFormatter.format(createDimTable, HologresTransformContext.builder().build(), HologresVersion.V1);
        assertEquals("BEGIN;\n"
            + "CREATE TABLE a (\n"
            + "   a TIMESTAMP,\n"
            + "   b TEXT PRIMARY KEY\n"
            + ");\n"
            + "COMMENT ON TABLE a IS 'comment';\n"
            + "COMMENT ON COLUMN a.a IS 'hello';\n"
            + "COMMIT;", format.getNode());
    }

    @Test
    public void testAliasedName() {
        List<ColumnDefinition> columns = ImmutableList.of(
            ColumnDefinition.builder().colName(new Identifier("co1")).aliasedName(new AliasedName("alias"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT)).build()
        );
        CreateDimTable createDimTable = CreateDimTable.builder().tableName(QualifiedName.of("a")).aliasedName(
            new AliasedName("col_alias")).columns(columns).build();
        DialectNode dialectNode = HologresFormatter.format(createDimTable, HologresTransformContext.builder().build(), HologresVersion.V1);
        assertEquals(dialectNode.getNode(), "BEGIN;\n"
            + "CREATE TABLE a (\n"
            + "   co1 BIGINT\n"
            + ");\n\n"
            + "COMMIT;");
    }

    @Test
    public void format_withPartitionTable() {
        CreateDimTable createDimTable = CreateDimTable.builder()
            .tableName(QualifiedName.of("a"))
            .columns(
                ImmutableList.of(
                    ColumnDefinition.builder().colName(new Identifier("a")).dataType(
                        DataTypeUtil.simpleType(DataTypeEnums.DATETIME)
                    ).comment(new Comment("hello")).build(),
                    ColumnDefinition.builder().colName(new Identifier("b")).dataType(
                        DataTypeUtil.simpleType(DataTypeEnums.STRING)
                    ).primary(true).build()
                )
            ).comment(new Comment("comment")).partition(
                new PartitionedBy(
                    ImmutableList.of(ColumnDefinition.builder().colName(new Identifier("p1"))
                        .comment(new Comment("comment"))
                        .dataType(DataTypeUtil.simpleType(DataTypeEnums.DATETIME))
                        .build())
                )
            ).constraints(ImmutableList.of(new PrimaryConstraint(new Identifier("sys"),
                ImmutableList.of(new Identifier("a"), new Identifier("b"))))).build();
        DialectNode format = HologresFormatter.format(createDimTable, HologresTransformContext.builder().build(), HologresVersion.V1);
        assertEquals(format.getNode(), "BEGIN;\n"
            + "CREATE TABLE a (\n"
            + "   a  TIMESTAMP,\n"
            + "   b  TEXT PRIMARY KEY,\n"
            + "   p1 TIMESTAMP,\n"
            + "   PRIMARY KEY(a,b)\n"
            + ") PARTITION BY LIST(p1);\n"
            + "COMMENT ON TABLE a IS 'comment';\n"
            + "COMMENT ON COLUMN a.a IS 'hello';\n"
            + "COMMENT ON COLUMN a.p1 IS 'comment';\n"
            + "COMMIT;");
    }

    @Test
    public void testRenameTable() {
        RenameTable renameTable = new RenameTable(QualifiedName.of("a"), QualifiedName.of("b"));
        DialectNode format = HologresFormatter.format(renameTable, HologresTransformContext.builder().build(), HologresVersion.V1);
        assertEquals(format.getNode(), "ALTER TABLE a RENAME TO b");

    }

    @Test
    public void testAddColumn() {
        AddCols addCols = new AddCols(
            QualifiedName.of("a.b"),
            ImmutableList.of(ColumnDefinition.builder().colName(new Identifier("new_column")).dataType(
                DataTypeUtil.simpleType(DataTypeEnums.DATETIME)
            ).comment(new Comment("comment")).build(), ColumnDefinition.builder().colName(new Identifier("new_column2"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT)).build())
        );
        DialectNode format = HologresFormatter.format(addCols, HologresTransformContext.builder().build(), HologresVersion.V1);
        assertEquals("BEGIN;\n"
            + "ALTER TABLE IF EXISTS b ADD COLUMN new_column TIMESTAMP, ADD COLUMN new_column2 BIGINT;\n"
            + "COMMENT ON COLUMN b.new_column IS 'comment';\n"
            + "COMMIT;", format.getNode());
    }

    @Test
    public void testDropTable() {
        DropTable dropTable = new DropTable(QualifiedName.of("b"));
        DialectNode format = HologresFormatter.format(dropTable, HologresTransformContext.builder().build(), HologresVersion.V1);
        assertEquals(format.getNode(), "DROP TABLE IF EXISTS b");
    }

    @Test
    public void testSetProperties() {
        SetTableProperties setTableProperties = new SetTableProperties(QualifiedName.of("b"),
            ImmutableList
                .of(new Property("dictionary_encoding_columns", "a:on,b:auto"), new Property("comment", "abc")));
        DialectNode format = HologresFormatter.format(setTableProperties, HologresTransformContext.builder().build(), HologresVersion.V1);
        assertEquals(format.getNode(),
            "BEGIN;\n"
                + "CALL SET_TABLE_PROPERTY('b', 'dictionary_encoding_columns', '\"a:on,b:auto\"');\n"
                + "COMMIT;");
    }

    @Test
    public void testDropPartitionCol() {
        DropPartitionCol dropPartitionCol = new DropPartitionCol(QualifiedName.of("a"), new Identifier("c"));
        DialectNode dialectNode = HologresFormatter.format(dropPartitionCol,
            HologresTransformContext.builder().build(), HologresVersion.V1);
        assertFalse(dialectNode.isExecutable());
    }

    @Test
    public void testAddPartitionCol() {
        AddPartitionCol addPartitionCol = new AddPartitionCol(QualifiedName.of("a"),
            ColumnDefinition.builder().colName(new Identifier("a")).dataType(DataTypeUtil.simpleType(
                DataTypeEnums.STRING
            )).build());
        DialectNode dialectNode = HologresFormatter.format(addPartitionCol,
            HologresTransformContext.builder().build(), HologresVersion.V1);
        assertFalse(dialectNode.isExecutable());
    }

    @Test
    public void testSetPropertiesWithSchema() {
        SetTableProperties setTableProperties =  new SetTableProperties(
            QualifiedName.of("jiawatest"),
            ImmutableList.of(
                new Property("binlog.level", "none")
            )
        );
        DialectNode dialectNode = HologresFormatter.format(setTableProperties, HologresTransformContext.builder().database("public").schema("analyse").build(), HologresVersion.V1);
        assertEquals(dialectNode.getNode(), "BEGIN;\n"
            + "CALL SET_TABLE_PROPERTY('analyse.jiawatest', 'binlog.level', 'none');\n"
            + "COMMIT;");
    }
}
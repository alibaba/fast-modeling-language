/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.parser.visitor;

import java.util.List;

import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.table.ChangeCol;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.RenameTable;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableComment;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableProperties;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.hologres.context.HologresTransformContext;
import com.aliyun.fastmodel.transform.hologres.dialect.HologresVersion;
import com.aliyun.fastmodel.transform.hologres.parser.tree.datatype.HologresGenericDataType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/6/29
 */
public class HologresAstVisitorTest {

    @Test
    public void visitSetTableComment() {
        HologresAstVisitor hologresAstVisitor = new HologresAstVisitor(HologresTransformContext.builder()
            .build(), HologresVersion.V1);
        SetTableComment statement = new SetTableComment(
            QualifiedName.of("a.b"),
            new Comment(null)
        );
        hologresAstVisitor.visitSetTableComment(statement, 0);
        String string = hologresAstVisitor.getBuilder().toString();
        assertEquals(string, "BEGIN;\n"
            + "COMMENT ON TABLE b IS NULL;\n"
            + "COMMIT;");

    }

    @Test
    public void testVisitCreateTable() {
        HologresAstVisitor hologresAstVisitor = new HologresAstVisitor(HologresTransformContext.builder().build());
        List<ColumnDefinition> columns = ImmutableList.of(
            ColumnDefinition.builder()
                .colName(new Identifier("c1"))
                .dataType(new HologresGenericDataType("text"))
                .primary(true)
                .build()
        );
        PartitionedBy partitionBy = new PartitionedBy(
            columns
        );
        CreateTable node = CreateTable.builder()
            .tableName(QualifiedName.of("abc"))
            .columns(columns)
            .partition(partitionBy)
            .build();
        hologresAstVisitor.visitCreateTable(node, 0);
        String string = hologresAstVisitor.getBuilder().toString();
        assertEquals(string, "BEGIN;\n"
            + "CREATE TABLE abc (\n"
            + "   c1 TEXT PRIMARY KEY\n"
            + ") PARTITION BY LIST(c1);\n\n"
            + "COMMIT;");
    }

    @Test
    public void testSetCommentWith() {
        HologresAstVisitor hologresAstVisitor = new HologresAstVisitor(HologresTransformContext.builder()
            .build());
        SetTableComment statement = new SetTableComment(
            QualifiedName.of("a.b"),
            new Comment("abc")
        );
        hologresAstVisitor.visitSetTableComment(statement, 0);
        String string = hologresAstVisitor.getBuilder().toString();
        assertEquals(string, "BEGIN;\n"
            + "COMMENT ON TABLE b IS 'abc';\n"
            + "COMMIT;");
    }

    @Test
    public void testCommentColumn() {
        Identifier oldColName = new Identifier("a.b");
        ChangeCol changeCol = new ChangeCol(
            QualifiedName.of("a.b"),
            oldColName,
            ColumnDefinition.builder()
                .colName(oldColName)
                .comment(new Comment("comment"))
                .build()
        );
        HologresAstVisitor hologresAstVisitor = new HologresAstVisitor(HologresTransformContext.builder()
            .build());
        hologresAstVisitor.visitChangeCol(changeCol, 0);
        String string = hologresAstVisitor.getBuilder().toString();
        assertEquals(string, "BEGIN;\n"
            + "COMMENT ON COLUMN b.\"a.b\" IS 'comment';\n"
            + "COMMIT;");
    }

    @Test
    public void testHologresConvert() {
        HologresAstVisitor hologresAstVisitor = new HologresAstVisitor(HologresTransformContext.builder().build());
        List<ColumnDefinition> columns = ImmutableList.of(
            ColumnDefinition.builder()
                .colName(new Identifier("c1"))
                .dataType(DataTypeUtil.simpleType("struct", null))
                .primary(true)
                .build(),
            ColumnDefinition.builder()
                .colName(new Identifier("c2"))
                .dataType(DataTypeUtil.simpleType("array", null))
                .primary(true)
                .build(),
            ColumnDefinition.builder()
                .colName(new Identifier("c3"))
                .dataType(DataTypeUtil.simpleType("string", null))
                .primary(true)
                .build()
        );
        PartitionedBy partitionBy = new PartitionedBy(
            columns
        );
        CreateTable node = CreateTable.builder()
            .tableName(QualifiedName.of("abc"))
            .columns(columns)
            .partition(partitionBy)
            .build();
        hologresAstVisitor.visitCreateTable(node, 0);
        String string = hologresAstVisitor.getBuilder().toString();
        assertEquals(string, "BEGIN;\n"
            + "CREATE TABLE abc (\n"
            + "   c1 JSON PRIMARY KEY,\n"
            + "   c2 JSON PRIMARY KEY,\n"
            + "   c3 TEXT PRIMARY KEY\n"
            + ") PARTITION BY LIST(c1,c2,c3);\n\n"
            + "COMMIT;");
    }

    @Test
    public void testSetProperties() {
        HologresAstVisitor hologresAstVisitor = new HologresAstVisitor(HologresTransformContext.builder().build(), HologresVersion.V2);
        SetTableProperties properties = new SetTableProperties(
            QualifiedName.of("tbl"),
            Lists.newArrayList(new Property("dictionary_encoding_columns", "C1:on,c2:off"))
        );
        hologresAstVisitor.visitSetTableProperties(properties, 0);
        String s = hologresAstVisitor.getBuilder().toString();
        assertEquals("BEGIN;\n"
            + "CALL SET_TABLE_PROPERTY('tbl', 'dictionary_encoding_columns', '\"C1\":on,\"c2\":off');\n"
            + "COMMIT;", s);
    }

    @Test
    public void testSetPropertiesV1() {
        HologresAstVisitor hologresAstVisitor = new HologresAstVisitor(HologresTransformContext.builder().build(), HologresVersion.V1);
        SetTableProperties properties = new SetTableProperties(
            QualifiedName.of("tbl"),
            Lists.newArrayList(new Property("dictionary_encoding_columns", "c1:on,C2:off"))
        );
        hologresAstVisitor.visitSetTableProperties(properties, 0);
        String s = hologresAstVisitor.getBuilder().toString();
        assertEquals("BEGIN;\n"
            + "CALL SET_TABLE_PROPERTY('tbl', 'dictionary_encoding_columns', '\"c1:on,C2:off\"');\n"
            + "COMMIT;", s);
    }

    @Test
    public void testSetPropertiesNotColumn() {
        HologresAstVisitor hologresAstVisitor = new HologresAstVisitor(HologresTransformContext.builder().build(), HologresVersion.V2);
        SetTableProperties properties = new SetTableProperties(
            QualifiedName.of("tbl"),
            Lists.newArrayList(new Property("binlog.ttl", "1000"))
        );
        hologresAstVisitor.visitSetTableProperties(properties, 0);
        String s = hologresAstVisitor.getBuilder().toString();
        assertEquals("BEGIN;\n"
            + "CALL SET_TABLE_PROPERTY('tbl', 'binlog.ttl', '1000');\n"
            + "COMMIT;", s);
    }

    @Test
    public void testDefaultValue() {
        HologresAstVisitor hologresAstVisitor = new HologresAstVisitor(HologresTransformContext.builder().build(), HologresVersion.V2);
        ColumnDefinition build = ColumnDefinition.builder()
            .colName(new Identifier("c1"))
            .dataType(DataTypeUtil.simpleType("string", null))
            .primary(true)
            .defaultValue(new StringLiteral("abc"))
            .build();
        hologresAstVisitor.visitColumnDefine(build, 0);
        String string = hologresAstVisitor.getBuilder().toString();
        assertEquals("c1 TEXT PRIMARY KEY DEFAULT 'abc'", string);
    }

    @Test
    public void testVisitChangeCol() {
        HologresAstVisitor hologresAstVisitor = new HologresAstVisitor(HologresTransformContext.builder().build(), HologresVersion.V2);
        ChangeCol changeCol1 = new ChangeCol(
            QualifiedName.of("t1"),
            new Identifier("c1"),
            ColumnDefinition.builder()
                .colName(new Identifier("c1"))
                .dataType(DataTypeUtil.simpleType("string", null))
                .defaultValue(new StringLiteral("st"))
                .build()
        );
        Boolean b = hologresAstVisitor.visitChangeCol(changeCol1, 0);
        assertEquals("BEGIN;\n"
            + "ALTER TABLE t1 ALTER COLUMN c1 SET DEFAULT 'st';\n"
            + "COMMIT;", hologresAstVisitor.getBuilder().toString());
    }

    @Test
    public void testRenameTable() {
        RenameTable renameTable = new RenameTable(QualifiedName.of("s1.t1"), QualifiedName.of("s1.t2"));
        HologresAstVisitor hologresAstVisitor = new HologresAstVisitor(HologresTransformContext.builder().schema("s1").build(), HologresVersion.V2);
        hologresAstVisitor.visitRenameTable(renameTable, 0);
        assertEquals("ALTER TABLE s1.t1 RENAME TO t2", hologresAstVisitor.getBuilder().toString());
    }
}
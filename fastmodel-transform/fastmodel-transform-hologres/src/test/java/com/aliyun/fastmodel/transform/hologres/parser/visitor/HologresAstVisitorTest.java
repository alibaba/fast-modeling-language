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
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ChangeCol;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableComment;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.hologres.context.HologresTransformContext;
import com.aliyun.fastmodel.transform.hologres.parser.tree.datatype.HologresDataTypeName;
import com.aliyun.fastmodel.transform.hologres.parser.tree.datatype.HologresGenericDataType;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.*;

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
            .build());
        SetTableComment statement = new SetTableComment(
            QualifiedName.of("a.b"),
            new Comment(null)
        );
        hologresAstVisitor.visitSetTableComment(statement, 0);
        String string = hologresAstVisitor.getBuilder().toString();
        assertEquals(string, "COMMENT ON TABLE b IS NULL;");

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
            + ") PARTITION BY LIST(c1);\n"
            + "CALL SET_TABLE_PROPERTY('abc', 'orientation', 'column');\n"
            + "CALL SET_TABLE_PROPERTY('abc', 'time_to_live_in_seconds', '3153600000');\n"
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
        assertEquals(string, "COMMENT ON TABLE b IS 'abc';");
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
        assertEquals(string, "COMMENT ON COLUMN b.\"a.b\" IS 'comment';");
    }
}
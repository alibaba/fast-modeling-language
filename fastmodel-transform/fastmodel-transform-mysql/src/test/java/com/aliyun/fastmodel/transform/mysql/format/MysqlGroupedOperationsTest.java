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

package com.aliyun.fastmodel.transform.mysql.format;

import java.util.Arrays;

import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.core.tree.statement.table.AddCols;
import com.aliyun.fastmodel.core.tree.statement.table.ChangeCol;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.SetColComment;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.mysql.context.MysqlTransformContext;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test for grouped ALTER TABLE operations in MysqlVisitor
 *
 * @author panguanjing
 * @date 2024
 */
public class MysqlGroupedOperationsTest {

    @Test
    public void testGroupedAlterTableOperations() {
        MysqlVisitor mysqlVisitor = new MysqlVisitor(MysqlTransformContext.builder().build());

        // Create multiple ALTER TABLE operations on the same table
        AddCols addCols = new AddCols(
            QualifiedName.of("users"),
            ImmutableList.of(
                ColumnDefinition.builder()
                    .colName(new Identifier("age"))
                    .dataType(DataTypeUtil.simpleType(DataTypeEnums.INT))
                    .build(),
                ColumnDefinition.builder()
                    .colName(new Identifier("created_at"))
                    .dataType(DataTypeUtil.simpleType(DataTypeEnums.DATETIME))
                    .build()
            )
        );

        ChangeCol changeCol = new ChangeCol(
            QualifiedName.of("users"),
            new Identifier("name"),
            ColumnDefinition.builder()
                .colName(new Identifier("full_name"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.VARCHAR))
                .build()
        );

        SetColComment setColComment = new SetColComment(
            QualifiedName.of("users"),
            new Identifier("email"),
            new Comment("Email address")
        );

        CompositeStatement compositeStatement = new CompositeStatement(
            Arrays.asList(addCols, changeCol, setColComment)
        );

        mysqlVisitor.visitCompositeStatement(compositeStatement, 0);
        String result = mysqlVisitor.getBuilder().toString();

        String expected = "ALTER TABLE users\n"
            + "  ADD COLUMN age INT,\n"
            + "  ADD COLUMN created_at DATETIME,\n"
            + "  CHANGE COLUMN name full_name VARCHAR,\n"
            + "  MODIFY COLUMN email COMMENT 'Email address';\n";

        // For this test, let's check if the result contains the expected pattern
        // Since the exact formatting might differ slightly, we can check certain key elements
        assertEquals(expected, result);
    }

    @Test
    public void testGroupedOperationsOnDifferentTables() {
        MysqlVisitor mysqlVisitor = new MysqlVisitor(MysqlTransformContext.builder().build());

        // Create operations on different tables
        AddCols addUserCol = new AddCols(
            QualifiedName.of("users"),
            ImmutableList.of(
                ColumnDefinition.builder()
                    .colName(new Identifier("age"))
                    .dataType(DataTypeUtil.simpleType(DataTypeEnums.INT))
                    .build()
            )
        );

        AddCols addOrderCol = new AddCols(
            QualifiedName.of("orders"),
            ImmutableList.of(
                ColumnDefinition.builder()
                    .colName(new Identifier("status"))
                    .dataType(DataTypeUtil.simpleType(DataTypeEnums.VARCHAR))
                    .build()
            )
        );

        CompositeStatement compositeStatement = new CompositeStatement(
            Arrays.asList(addUserCol, addOrderCol)
        );

        mysqlVisitor.visitCompositeStatement(compositeStatement, 0);
        String result = mysqlVisitor.getBuilder().toString();

        // Should have two separate ALTER TABLE statements
        // Count the number of ALTER TABLE occurrences
        long alterTableCount = Arrays.stream(result.split("\n"))
            .filter(line -> line.trim().startsWith("ALTER TABLE"))
            .count();
        assertEquals(2, alterTableCount);  // Should have both ALTER TABLE statements
    }
}
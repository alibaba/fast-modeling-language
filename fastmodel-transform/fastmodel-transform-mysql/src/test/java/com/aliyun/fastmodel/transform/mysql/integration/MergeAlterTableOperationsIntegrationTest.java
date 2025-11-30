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

package com.aliyun.fastmodel.transform.mysql.integration;

import java.util.Arrays;

import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.core.tree.statement.table.AddCols;
import com.aliyun.fastmodel.core.tree.statement.table.ChangeCol;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.DropCol;
import com.aliyun.fastmodel.core.tree.statement.table.SetColComment;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.mysql.MysqlTransformer;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Integration test for DefaultCodeGenerator to verify that ALTER TABLE operations
 * are merged when table changes occur
 *
 * @author panguanjing
 * @date 2024
 */
public class MergeAlterTableOperationsIntegrationTest {

    /**
     * Tests that when multiple column operations are performed on the same table,
     * the generated SQL combines them into a single ALTER TABLE statement
     */
    @Test
    public void testMergedAlterTableOperations() {
        // Create multiple column operations on the same table
        AddCols addCol1 = new AddCols(
            QualifiedName.of("users"),
            ImmutableList.of(
                ColumnDefinition.builder()
                    .colName(new Identifier("age"))
                    .dataType(DataTypeUtil.simpleType(DataTypeEnums.INT))
                    .build()
            )
        );

        AddCols addCol2 = new AddCols(
            QualifiedName.of("users"),
            ImmutableList.of(
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

        DropCol dropCol = new DropCol(
            QualifiedName.of("users"),
            new Identifier("old_field")
        );

        // Create a composite statement with all operations
        CompositeStatement compositeStatement = new CompositeStatement(
            Arrays.asList(addCol1, addCol2, changeCol, setColComment, dropCol)
        );

        // Use the MySQL transformer directly to test the merged operations
        MysqlTransformer mysqlTransformer = new MysqlTransformer();
        TransformContext context = TransformContext.builder().build();
        DialectNode result = mysqlTransformer.transform(compositeStatement, context);

        String generatedSql = result.getNode();
        System.out.println("Generated SQL: " + generatedSql);

        // Verify that the generated SQL contains a single ALTER TABLE statement
        // with multiple operations separated by commas
        assertNotNull(generatedSql);
        assertEquals(1, countOccurrences(generatedSql, "ALTER TABLE users"));

        // Verify that all operations are present in the single ALTER TABLE statement
        assertTrue(generatedSql.contains("ADD COLUMN"));
        assertTrue(generatedSql.contains("CHANGE COLUMN"));
        assertTrue(generatedSql.contains("MODIFY COLUMN"));
        assertTrue(generatedSql.contains("DROP COLUMN"));

    }

    /**
     * Tests that operations on different tables are not merged
     */
    @Test
    public void testDifferentTablesNotMerged() {
        // Operations on different tables
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

        MysqlTransformer mysqlTransformer = new MysqlTransformer();
        TransformContext context = TransformContext.builder().build();
        DialectNode result = mysqlTransformer.transform(compositeStatement, context);

        String generatedSql = result.getNode();

        // Should have two ALTER TABLE statements since operations are on different tables
        int userTableCount = countOccurrences(generatedSql, "ALTER TABLE users");
        int ordersTableCount = countOccurrences(generatedSql, "ALTER TABLE orders");

        assertEquals("Should have ALTER TABLE for users", 1, userTableCount);
        assertEquals("Should have ALTER TABLE for orders", 1, ordersTableCount);
    }

    /**
     * Helper method to count occurrences of a substring
     */
    private int countOccurrences(String text, String subString) {
        if (text == null || subString == null || subString.isEmpty()) {
            return 0;
        }
        int count = 0;
        int index = 0;
        while ((index = text.indexOf(subString, index)) != -1) {
            count++;
            index += subString.length();
        }
        return count;
    }

    /**
     * Simple helper method to avoid importing external libraries
     */
    private void assertTrue(boolean condition) {
        if (!condition) {
            throw new AssertionError("Expected true but was false");
        }
    }
}
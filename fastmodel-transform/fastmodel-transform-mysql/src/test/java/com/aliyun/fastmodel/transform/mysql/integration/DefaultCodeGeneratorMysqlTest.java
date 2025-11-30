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

import java.util.List;

import com.aliyun.fastmodel.transform.api.client.dto.request.DdlGeneratorModelRequest;
import com.aliyun.fastmodel.transform.api.client.dto.result.DdlGeneratorResult;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.client.dto.table.TableConfig;
import com.aliyun.fastmodel.transform.api.client.generator.DefaultCodeGenerator;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.api.dialect.IVersion;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for DefaultCodeGenerator using MySQL dialect
 *
 * @author panguanjing
 * @date 2024
 */
public class DefaultCodeGeneratorMysqlTest {

    /**
     * Tests DefaultCodeGenerator with MySQL dialect for simple operations
     */
    @Test
    public void testDefaultCodeGeneratorWithMysql() {

        DefaultCodeGenerator generator = new DefaultCodeGenerator();

        TableConfig config = TableConfig.builder()
            .dialectMeta(new DialectMeta(DialectName.MYSQL, IVersion.getDefault()))
            .build();

        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .after(Table.builder()
                .name("users")
                .database("test")
                .columns(ImmutableList.of(
                    Column.builder()
                        .name("id")
                        .dataType("BIGINT")
                        .build(),
                    Column.builder()
                        .name("name")
                        .dataType("VARCHAR")
                        .build()
                ))
                .build())
            .config(config)
            .build();

        DdlGeneratorResult result = generator.generate(request);

        assertNotNull(result);
        assertNotNull(result.getDialectNodes());
        assertTrue(result.getDialectNodes().size() > 0);
    }

    /**
     * Tests DefaultCodeGenerator with MySQL dialect for composite statements (ALTER TABLE operations)
     */
    @Test
    public void testDefaultCodeGeneratorWithIncrementalSql() {
        // Use the DefaultCodeGenerator with MySQL dialect to generate incremental changes
        DefaultCodeGenerator generator = new DefaultCodeGenerator();

        TableConfig config = TableConfig.builder()
            .dialectMeta(new DialectMeta(DialectName.MYSQL, IVersion.getDefault()))
            .build();

        // Define 'before' table state (initial state with minimal columns)
        Table beforeTable =
            Table.builder()
                .name("users")
                .database("test")
                .columns(ImmutableList.of(
                    Column.builder()
                        .name("id")
                        .dataType("BIGINT")
                        .build(),
                    Column.builder()
                        .name("age")
                        .dataType("VARCHAR")
                        .build()
                ))
                .build();

        // Define 'after' table state (final state with additional columns)
        Table afterTable =
            Table.builder()
                .name("users")
                .database("test")
                .columns(ImmutableList.of(
                    Column.builder()
                        .name("id")
                        .dataType("BIGINT")
                        .build(),
                    Column.builder()
                        .name("age")
                        .dataType("INT")
                        .comment("User age")
                        .build(),
                    Column.builder()
                        .name("created_at")
                        .dataType("DATETIME")
                        .comment("Creation timestamp")
                        .build(),
                    Column.builder()
                        .name("full_name")
                        .dataType("VARCHAR")
                        .comment("Full name of user")
                        .build()
                ))
                .build();

        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .before(beforeTable)  // before table state
            .after(afterTable)    // after table state
            .config(config)
            .build();

        DdlGeneratorResult result = generator.generate(request);

        // The result should contain incremental statements to transform from 'before' to 'after'
        assertNotNull(result);
        assertNotNull(result.getDialectNodes());
        assertTrue("Should generate at least one DDL statement", result.getDialectNodes().size() > 0);

        // Print the generated incremental SQL for verification
        System.out.println("Incremental SQL generated by DefaultCodeGenerator:");
        result.getDialectNodes().forEach(dialectNode -> {
            String sql = dialectNode.getNode();
            System.out.println(sql);

            // Verify that the generated SQL is MySQL-specific
            assertTrue("Should contain ALTER TABLE for MySQL", sql.toUpperCase().contains("ALTER TABLE"));
            assertTrue("Should target the correct table", sql.contains("users"));
        });

        // Verify that the generated SQL includes ADD COLUMN operations for the new columns
        boolean hasAddColumn = result.getDialectNodes().stream()
            .anyMatch(dialectNode -> dialectNode.getNode().toUpperCase().contains("ADD COLUMN"));
        assertTrue("Generated SQL should contain ADD COLUMN statements for incremental changes", hasAddColumn);
    }

    /**
     * Tests that DefaultCodeGenerator properly handles MySQL-specific transformations
     */
    @Test
    public void testMysqlSpecificTransformations() {
        DefaultCodeGenerator generator = new DefaultCodeGenerator();

        TableConfig config = TableConfig.builder()
            .dialectMeta(new DialectMeta(DialectName.MYSQL, IVersion.getDefault()))
            .build();

        // Define 'before' table state (table with email column to be dropped)
        Table beforeTable =
            Table.builder()
                .name("users")
                .database("test")
                .columns(ImmutableList.of(
                    Column.builder()
                        .name("id")
                        .dataType("BIGINT")
                        .build(),
                    Column.builder()
                        .name("email")
                        .dataType("VARCHAR")
                        .comment("Email address")
                        .build()
                ))
                .build();

        // Define 'after' table state (table without email column, but with new columns)
        Table afterTable =
            Table.builder()
                .name("users")
                .database("test")
                .columns(ImmutableList.of(
                    Column.builder()
                        .name("id")
                        .dataType("BIGINT")
                        .build(),
                    Column.builder()
                        .name("username")
                        .dataType("VARCHAR")
                        .comment("Username")
                        .build()
                ))
                .build();

        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .before(beforeTable)
            .after(afterTable)
            .config(config)
            .build();

        DdlGeneratorResult result = generator.generate(request);

        assertNotNull(result);
        List<com.aliyun.fastmodel.transform.api.dialect.DialectNode> dialectNodes = result.getDialectNodes();
        assertTrue("Should have at least one dialect node", dialectNodes.size() > 0);

        System.out.println("MySQL-specific transformations (before->after changes):");
        for (com.aliyun.fastmodel.transform.api.dialect.DialectNode node : dialectNodes) {
            String sql = node.getNode();
            System.out.println(sql);

            // Verify it's using MySQL dialect
            assertTrue("Should contain MySQL dialect syntax", sql.toLowerCase().contains("alter table"));
        }
    }

    /**
     * Tests that multiple ALTER TABLE operations on the same table are merged into a single statement
     */
    @Test
    public void testMergedAlterTableOperationsWithDefaultCodeGenerator() {
        DefaultCodeGenerator generator = new DefaultCodeGenerator();

        TableConfig config = TableConfig.builder()
            .dialectMeta(new DialectMeta(DialectName.MYSQL, IVersion.getDefault()))
            .mergeAlterTableOperations(true)
            .build();

        // Define 'before' table state (table with a few columns)
        Table beforeTable =
            Table.builder()
                .name("users")
                .database("test")
                .columns(ImmutableList.of(
                    Column.builder()
                        .name("id")
                        .dataType("BIGINT")
                        .build(),
                    Column.builder()
                        .name("old_name")
                        .dataType("VARCHAR")
                        .comment("Old name field")
                        .build()
                ))
                .build();

        // Define 'after' table state (table with modifications)
        Table afterTable =
            Table.builder()
                .name("users")
                .database("test")
                .columns(ImmutableList.of(
                    Column.builder()
                        .name("id")
                        .dataType("BIGINT")
                        .build(),
                    Column.builder()
                        .name("full_name")
                        .dataType("VARCHAR")
                        .comment("Full name")
                        .build(),
                    Column.builder()
                        .name("email")
                        .dataType("VARCHAR")
                        .comment("Email address")
                        .build(),
                    Column.builder()
                        .name("age")
                        .dataType("INT")
                        .comment("Age of user")
                        .build()
                ))
                .build();

        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .before(beforeTable)
            .after(afterTable)
            .config(config)
            .build();

        DdlGeneratorResult result = generator.generate(request);

        assertNotNull(result);
        List<com.aliyun.fastmodel.transform.api.dialect.DialectNode> dialectNodes = result.getDialectNodes();
        assertTrue("Should have generated statements", dialectNodes.size() == 1);

        System.out.println("Checking for merged ALTER TABLE operations:");
        for (com.aliyun.fastmodel.transform.api.dialect.DialectNode node : dialectNodes) {
            String sql = node.getNode();
            System.out.println(sql);

            // Verify it's using MySQL dialect
            assertTrue("Should contain MySQL dialect syntax", sql.toLowerCase().contains("alter table"));
        }

        // Count how many ALTER TABLE statements target the same table
        long alterTableCount = dialectNodes.stream()
            .map(node -> node.getNode())
            .filter(sql -> sql.toUpperCase().contains("ALTER TABLE"))
            .filter(sql -> sql.contains("users"))
            .count();

        System.out.println("Number of ALTER TABLE statements for 'users': " + alterTableCount);
    }
}
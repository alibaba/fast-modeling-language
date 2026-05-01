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

package com.aliyun.fastmodel.transform.starrocks.parser;

import java.util.List;

import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.index.TableIndex;
import com.aliyun.fastmodel.transform.starrocks.context.StarRocksContext;
import com.aliyun.fastmodel.transform.starrocks.format.StarRocksOutVisitor;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * StarRocks DDL 兼容性修复用例集合（aone #81289947 扩展修复）。
 */
public class StarRocksCompatFixTest {

    private final StarRocksLanguageParser parser = new StarRocksLanguageParser();

    @Test
    public void testDecimalV3() {
        Node node = parser.parseNode("CREATE TABLE t (v DECIMALV3(20, 5));");
        assertNotNull(node);
        CreateTable createTable = (CreateTable) node;
        ColumnDefinition col = createTable.getColumnDefines().get(0);
        assertTrue(col.getDataType().getTypeName().getValue().contains("DECIMALV3"));
    }

    @Test
    public void testIndexUsingNgramBloomFilter() {
        Node node = parser.parseNode("CREATE TABLE t (k STRING, INDEX idx_k (k) USING NGRAM_BF);");
        assertNotNull(node);
        CreateTable createTable = (CreateTable) node;
        List<TableIndex> indexes = createTable.getTableIndexList();
        assertEquals(1, indexes.size());
        List<Property> props = indexes.get(0).getProperties();
        assertTrue(props.stream().anyMatch(p -> "NGRAM_BF".equals(p.getValue())));
    }

    @Test
    public void testIndexUsingGin() {
        Node node = parser.parseNode("CREATE TABLE t (k STRING, INDEX idx_k (k) USING GIN);");
        assertNotNull(node);
        CreateTable createTable = (CreateTable) node;
        List<Property> props = createTable.getTableIndexList().get(0).getProperties();
        assertTrue(props.stream().anyMatch(p -> "GIN".equals(p.getValue())));
    }

    @Test
    public void testIndexUsingBitmapUnchanged() {
        Node node = parser.parseNode("CREATE TABLE t (k INT, INDEX idx_k (k) USING BITMAP);");
        assertNotNull(node);
        List<Property> props = ((CreateTable) node).getTableIndexList().get(0).getProperties();
        assertTrue(props.stream().anyMatch(p -> "BITMAP".equals(p.getValue())));
    }

    @Test
    public void testBucketsAuto() {
        Node node = parser.parseNode("CREATE TABLE t (id INT) PRIMARY KEY(id) DISTRIBUTED BY HASH(id) BUCKETS AUTO;");
        assertNotNull(node);
    }

    @Test
    public void testBucketsAutoRandom() {
        Node node = parser.parseNode("CREATE TABLE t (id INT) DUPLICATE KEY(id) DISTRIBUTED BY RANDOM BUCKETS AUTO;");
        assertNotNull(node);
    }

    @Test
    public void testRangePartitionLessThanMaxvalueSingleColumn() {
        Node node = parser.parseNode("CREATE TABLE t (dt DATE) DUPLICATE KEY(dt) PARTITION BY RANGE(dt) (PARTITION p1 VALUES LESS THAN MAXVALUE);");
        assertNotNull(node);
    }

    @Test
    public void testDateV2AndDateTimeV2() {
        Node node = parser.parseNode("CREATE TABLE t (d DATEV2, ts DATETIMEV2(3));");
        assertNotNull(node);
        CreateTable createTable = (CreateTable) node;
        assertEquals(2, createTable.getColumnDefines().size());
        assertTrue(createTable.getColumnDefines().get(0).getDataType().getTypeName().getValue().contains("DATEV2"));
        assertTrue(createTable.getColumnDefines().get(1).getDataType().getTypeName().getValue().contains("DATETIMEV2"));
    }

    @Test
    public void testTemporaryTableAsSelect() {
        // grammar must accept; visitor may still return null since TEMPORARY TABLE
        // has no concrete AST mapping yet, but no parse exception should be thrown.
        parser.parseNode("CREATE TEMPORARY TABLE t AS SELECT 1 AS a;");
    }

    @Test
    public void testTemporaryTableWithColumns() {
        parser.parseNode("CREATE TEMPORARY TABLE t (a INT, b STRING) DUPLICATE KEY(a);");
    }

    @Test
    public void testOnUpdateCurrentTimestamp() {
        Node node = parser.parseNode("CREATE TABLE t (ts DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP);");
        assertNotNull(node);
    }

    @Test
    public void testTableTrailingCharset() {
        Node node = parser.parseNode("CREATE TABLE t (v INT) DUPLICATE KEY(v) DEFAULT CHARSET=utf8mb4;");
        assertNotNull(node);
    }

    @Test
    public void testTableTrailingCollate() {
        Node node = parser.parseNode("CREATE TABLE t (v INT) DUPLICATE KEY(v) DEFAULT COLLATE=utf8_bin;");
        assertNotNull(node);
    }

    @Test
    public void testTableTrailingCharsetAndCollate() {
        Node node = parser.parseNode("CREATE TABLE t (v INT) DUPLICATE KEY(v) DEFAULT CHARSET=utf8mb4 DEFAULT COLLATE=utf8mb4_bin;");
        assertNotNull(node);
    }

    @Test
    public void testPrimaryKeyNotEnforced() {
        Node node = parser.parseNode("CREATE TABLE t (id INT) PRIMARY KEY(id) NOT ENFORCED;");
        assertNotNull(node);
    }

    @Test
    public void testColumnUnique() {
        Node node = parser.parseNode("CREATE TABLE t (id INT UNIQUE);");
        assertNotNull(node);
    }

    @Test
    public void testColumnCheck() {
        Node node = parser.parseNode("CREATE TABLE t (id INT CHECK (id > 0));");
        assertNotNull(node);
    }

    @Test
    public void testColumnUniqueAndCheck() {
        Node node = parser.parseNode("CREATE TABLE t (id INT UNIQUE CHECK (id > 0) COMMENT 'pk');");
        assertNotNull(node);
    }

    @Test
    public void testBucketsAutoRoundTrip() {
        String input = "CREATE TABLE t (id INT) PRIMARY KEY(id) DISTRIBUTED BY HASH(id) BUCKETS AUTO;";
        Node node = parser.parseNode(input);
        StarRocksOutVisitor visitor = new StarRocksOutVisitor(StarRocksContext.builder().build());
        visitor.process(node, 0);
        String output = visitor.getBuilder().toString();
        assertTrue("expected output to contain 'BUCKETS AUTO', was: " + output,
            output.contains("BUCKETS AUTO"));
    }

    @Test
    public void testBucketsAutoRandomRoundTrip() {
        String input = "CREATE TABLE t (id INT) DUPLICATE KEY(id) DISTRIBUTED BY RANDOM BUCKETS AUTO;";
        Node node = parser.parseNode(input);
        StarRocksOutVisitor visitor = new StarRocksOutVisitor(StarRocksContext.builder().build());
        visitor.process(node, 0);
        String output = visitor.getBuilder().toString();
        assertTrue("expected output to contain 'BUCKETS AUTO', was: " + output,
            output.contains("BUCKETS AUTO"));
    }

    @Test
    public void testBucketsIntRoundTripUnchanged() {
        String input = "CREATE TABLE t (id INT) PRIMARY KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 10;";
        Node node = parser.parseNode(input);
        StarRocksOutVisitor visitor = new StarRocksOutVisitor(StarRocksContext.builder().build());
        visitor.process(node, 0);
        String output = visitor.getBuilder().toString();
        assertTrue("expected 'BUCKETS 10', was: " + output, output.contains("BUCKETS 10"));
        assertTrue("should not contain AUTO: " + output, !output.contains("AUTO"));
    }
}

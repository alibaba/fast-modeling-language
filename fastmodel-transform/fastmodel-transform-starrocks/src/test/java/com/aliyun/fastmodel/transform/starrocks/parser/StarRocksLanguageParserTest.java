package com.aliyun.fastmodel.transform.starrocks.parser;

import java.util.List;

import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.datatype.StarRocksDataTypeName;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.PartitionDesc;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.RangePartitionedBy;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2023/9/11
 */
public class StarRocksLanguageParserTest {
    StarRocksLanguageParser starRocksLanguageParser = new StarRocksLanguageParser();

    @Test
    public void parseNode() {
        Node node = starRocksLanguageParser.parseNode("CREATE TABLE example_db.table_hash\n"
            + "(\n"
            + "    k1 TINYINT,\n"
            + "    k2 DECIMAL(10, 2) DEFAULT \"10.5\",\n"
            + "    v1 CHAR(10) REPLACE,\n"
            + "    v2 INT SUM\n"
            + ")\n"
            + "ENGINE=olap\n"
            + "AGGREGATE KEY(k1, k2)\n"
            + "COMMENT \"my first starrocks table\"\n"
            + "DISTRIBUTED BY HASH(k1) BUCKETS 10\n"
            + "PROPERTIES (\"storage_type\"=\"column\");");
        assertNotNull(node);
        CreateTable createTable = (CreateTable)node;
        assertEquals("example_db.table_hash", createTable.getQualifiedName().toString());
        assertEquals(4, createTable.getColumnDefines().size());
    }

    @Test
    public void testParseNodeWithPartition() {
        Node node = starRocksLanguageParser.parseNode("CREATE TABLE example_db.table_range\n"
            + "(\n"
            + "    k1 DATE,\n"
            + "    k2 INT,\n"
            + "    k3 SMALLINT,\n"
            + "    v1 VARCHAR(2048),\n"
            + "    v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n"
            + ")\n"
            + "ENGINE=olap\n"
            + "DUPLICATE KEY(k1, k2, k3)\n"
            + "PARTITION BY RANGE (k1)\n"
            + "(\n"
            + "    PARTITION p1 VALUES LESS THAN (\"2014-01-01\"),\n"
            + "    PARTITION p2 VALUES LESS THAN (\"2014-06-01\"),\n"
            + "    PARTITION p3 VALUES LESS THAN (\"2014-12-01\")\n"
            + ")\n"
            + "DISTRIBUTED BY HASH(k2) BUCKETS 10\n"
            + "PROPERTIES(\n"
            + "    \"storage_medium\" = \"SSD\", \n"
            + "    \"storage_cooldown_time\" = \"2015-06-04 00:00:00\"\n"
            + ");");
        assertNotNull(node);
        CreateTable createTable = (CreateTable)node;
        PartitionedBy partitionedBy = createTable.getPartitionedBy();
        RangePartitionedBy starRocksPartitionedBy = (RangePartitionedBy)partitionedBy;
        List<PartitionDesc> rangePartitions = starRocksPartitionedBy.getRangePartitions();
        assertEquals(3, rangePartitions.size());
    }

    @Test
    public void testParseWithFixPartition() {
        Node node = starRocksLanguageParser.parseNode("CREATE TABLE table_range\n"
            + "(\n"
            + "    k1 DATE,\n"
            + "    k2 INT,\n"
            + "    k3 SMALLINT,\n"
            + "    v1 VARCHAR(2048),\n"
            + "    v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n"
            + ")\n"
            + "ENGINE=olap\n"
            + "DUPLICATE KEY(k1, k2, k3)\n"
            + "PARTITION BY RANGE (k1, k2, k3)\n"
            + "(\n"
            + "    PARTITION p1 VALUES [(\"2014-01-01\", \"10\", \"200\"), (\"2014-01-01\", \"20\", \"300\")),\n"
            + "    PARTITION p2 VALUES [(\"2014-06-01\", \"100\", \"200\"), (\"2014-07-01\", \"100\", \"300\"))\n"
            + ")\n"
            + "DISTRIBUTED BY HASH(k2) BUCKETS 10\n"
            + "PROPERTIES(\n"
            + "    \"storage_medium\" = \"SSD\"\n"
            + ");");
        assertNotNull(node);
    }

    @Test
    public void testParseWithHllColumns() {
        Node node = starRocksLanguageParser.parseNode("CREATE TABLE example_db.example_table\n"
            + "(\n"
            + "    k1 TINYINT,\n"
            + "    k2 DECIMAL(10, 2) DEFAULT \"10.5\",\n"
            + "    v1 HLL HLL_UNION,\n"
            + "    v2 HLL HLL_UNION\n"
            + ")\n"
            + "ENGINE=olap\n"
            + "AGGREGATE KEY(k1, k2)\n"
            + "DISTRIBUTED BY HASH(k1) BUCKETS 10\n"
            + "PROPERTIES (\"storage_type\"=\"column\");");
        assertNotNull(node);
    }

    @Test
    public void testWithBitMapUnion() {
        Node node = starRocksLanguageParser.parseNode("CREATE TABLE example_db.example_table\n"
            + "(\n"
            + "    k1 TINYINT,\n"
            + "    k2 DECIMAL(10, 2) DEFAULT \"10.5\",\n"
            + "    v1 BITMAP BITMAP_UNION,\n"
            + "    v2 BITMAP BITMAP_UNION\n"
            + ")\n"
            + "ENGINE=olap\n"
            + "AGGREGATE KEY(k1, k2)\n"
            + "DISTRIBUTED BY HASH(k1) BUCKETS 10\n"
            + "PROPERTIES (\"storage_type\"=\"column\");");
        assertNotNull(node);
    }

    @Test
    public void testParseWithIndex() {
        Node createTableNode = starRocksLanguageParser.parseNode("CREATE TABLE example_db.table_hash\n"
            + "(\n"
            + "    k1 TINYINT,\n"
            + "    k2 DECIMAL(10, 2) DEFAULT \"10.5\",\n"
            + "    v1 CHAR(10) REPLACE,\n"
            + "    v2 INT SUM,\n"
            + "    INDEX k1_idx (k1) USING BITMAP COMMENT 'xxxxxx'\n"
            + ")\n"
            + "ENGINE=olap\n"
            + "AGGREGATE KEY(k1, k2)\n"
            + "COMMENT \"my first starrocks table\"\n"
            + "DISTRIBUTED BY HASH(k1) BUCKETS 10\n"
            + "PROPERTIES (\"storage_type\"=\"column\");");
        assertNotNull(createTableNode);
    }

    @Test
    public void testParseDataType() {
        BaseDataType baseDataType = starRocksLanguageParser.parseDataType("array<int>", ReverseContext.builder().build());
        assertEquals(baseDataType.getTypeName().getValue(), StarRocksDataTypeName.ARRAY.getValue());
    }
}
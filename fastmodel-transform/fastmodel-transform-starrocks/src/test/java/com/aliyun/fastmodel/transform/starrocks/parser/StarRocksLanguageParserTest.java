package com.aliyun.fastmodel.transform.starrocks.parser;

import java.util.List;
import java.util.Optional;

import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.FunctionCall;
import com.aliyun.fastmodel.core.tree.expr.literal.CurrentTimestamp;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.constants.ConstraintType;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.UniqueConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.index.TableIndex;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.AggregateKeyConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.DuplicateKeyConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.desc.DistributeConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.desc.OrderByConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.ExpressionPartitionBy;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.RangePartitionedBy;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.desc.PartitionDesc;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.datatype.StarRocksDataTypeName;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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
        List<ColumnDefinition> columnDefines = createTable.getColumnDefines();
        Optional<ColumnDefinition> v2 = columnDefines.stream().filter(
            c -> StringUtils.equalsIgnoreCase(c.getColName().getValue(), "v2")
        ).findFirst();

        if (v2.isPresent()) {
            ColumnDefinition columnDefinition = v2.get();
            BaseExpression defaultValue = columnDefinition.getDefaultValue();
            StringLiteral stringLiteral = (StringLiteral)defaultValue;
            assertEquals("2014-02-04 15:36:00", stringLiteral.getValue());
        }

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

    /**
     * 明细模型
     */
    @Test
    public void testParseWithDetailModel() {
        Node node = starRocksLanguageParser.parseNode("CREATE TABLE IF NOT EXISTS detail (\n"
            + "    event_time DATETIME NOT NULL COMMENT \"datetime of event\",\n"
            + "    event_type INT NOT NULL COMMENT \"type of event\",\n"
            + "    user_id INT COMMENT \"id of user\",\n"
            + "    device_code INT COMMENT \"device code\",\n"
            + "    channel INT COMMENT \"\"\n"
            + ")\n"
            + "DUPLICATE KEY(event_time, event_type)\n"
            + "DISTRIBUTED BY HASH(user_id)\n"
            + "PROPERTIES (\n"
            + "\"replication_num\" = \"3\"\n"
            + ");");
        CreateTable createTable = (CreateTable)node;
        List<BaseConstraint> constraintStatements = createTable.getConstraintStatements();
        boolean duplicate = false;
        for (BaseConstraint baseConstraint : constraintStatements) {
            if (baseConstraint instanceof DuplicateKeyConstraint) {
                DuplicateKeyConstraint duplicateConstraint = (DuplicateKeyConstraint)baseConstraint;
                duplicate = true;
                assertEquals(duplicateConstraint.getCustomType(), DuplicateKeyConstraint.TYPE);
                assertEquals(2, duplicateConstraint.getColumns().size());
            }
        }
        assertTrue(duplicate);
    }

    /**
     * 聚合模型
     */
    @Test
    public void testParseWithAggregateModel() {
        Node node = starRocksLanguageParser.parseNode("CREATE TABLE IF NOT EXISTS example_db.aggregate_tbl (\n"
            + "    site_id LARGEINT NOT NULL COMMENT \"id of site\",\n"
            + "    date DATE NOT NULL COMMENT \"time of event\",\n"
            + "    city_code VARCHAR(20) COMMENT \"city_code of user\",\n"
            + "    pv BIGINT SUM DEFAULT \"0\" COMMENT \"total page views\"\n"
            + ")\n"
            + "AGGREGATE KEY(site_id, date, city_code)\n"
            + "DISTRIBUTED BY HASH(site_id)\n"
            + "PROPERTIES (\n"
            + "\"replication_num\" = \"3\"\n"
            + ");");
        CreateTable createTable = (CreateTable)node;
        boolean hasAggr = false;
        for (BaseConstraint baseConstraint : createTable.getConstraintStatements()) {
            if (baseConstraint instanceof AggregateKeyConstraint) {
                AggregateKeyConstraint aggregateConstraint = (AggregateKeyConstraint)baseConstraint;
                hasAggr = true;
                assertEquals(aggregateConstraint.getCustomType(), AggregateKeyConstraint.TYPE);
                assertEquals(3, aggregateConstraint.getColumns().size());
            }
        }
        assertTrue(hasAggr);
    }

    /**
     * 更新模型
     */
    @Test
    public void testParseWithUpdateModel() {
        Node node = starRocksLanguageParser.parseNode("CREATE TABLE IF NOT EXISTS orders (\n"
            + "    create_time DATE NOT NULL COMMENT \"create time of an order\",\n"
            + "    order_id BIGINT NOT NULL COMMENT \"id of an order\",\n"
            + "    order_state INT COMMENT \"state of an order\",\n"
            + "    total_price BIGINT COMMENT \"price of an order\"\n"
            + ")\n"
            + "UNIQUE KEY(create_time, order_id)\n"
            + "DISTRIBUTED BY HASH(order_id)\n"
            + "PROPERTIES (\n"
            + "\"replication_num\" = \"3\"\n"
            + "); ");
        CreateTable createTable = (CreateTable)node;
        boolean uniq = false;
        for (BaseConstraint baseConstraint : createTable.getConstraintStatements()) {
            if (baseConstraint instanceof UniqueConstraint) {
                UniqueConstraint uniqueConstraint = (UniqueConstraint)baseConstraint;
                uniq = true;
                assertEquals(uniqueConstraint.getConstraintType(), ConstraintType.UNIQUE);
                assertEquals(2, uniqueConstraint.getColumnNames().size());
            }
        }
        assertTrue(uniq);
    }

    @Test
    public void testParseWithDistribute() {
        Node node = starRocksLanguageParser.parseNode("CREATE TABLE IF NOT EXISTS orders (\n"
            + "    create_time DATE NOT NULL COMMENT \"create time of an order\",\n"
            + "    order_id BIGINT NOT NULL COMMENT \"id of an order\",\n"
            + "    order_state INT COMMENT \"state of an order\",\n"
            + "    total_price BIGINT COMMENT \"price of an order\"\n"
            + ")\n"
            + "UNIQUE KEY(create_time, order_id)\n"
            + "DISTRIBUTED BY HASH(order_id)\n"
            + "PROPERTIES (\n"
            + "\"replication_num\" = \"3\"\n"
            + "); ");
        CreateTable createTable = (CreateTable)node;
        boolean distributeKey = false;
        for (BaseConstraint baseConstraint : createTable.getConstraintStatements()) {
            if (baseConstraint instanceof DistributeConstraint) {
                DistributeConstraint uniqueConstraint = (DistributeConstraint)baseConstraint;
                distributeKey = true;
                assertEquals(uniqueConstraint.getCustomType(), DistributeConstraint.TYPE);
                assertEquals(1, uniqueConstraint.getColumns().size());
            }
        }
        assertTrue(distributeKey);
    }

    @Test
    public void testParseWithDistributeBucket() {
        Node node = starRocksLanguageParser.parseNode("CREATE TABLE IF NOT EXISTS orders (\n"
            + "    create_time DATE NOT NULL COMMENT \"create time of an order\",\n"
            + "    order_id BIGINT NOT NULL COMMENT \"id of an order\",\n"
            + "    order_state INT COMMENT \"state of an order\",\n"
            + "    total_price BIGINT COMMENT \"price of an order\"\n"
            + ")\n"
            + "UNIQUE KEY(create_time, order_id)\n"
            + "DISTRIBUTED BY HASH(order_id) buckets 3\n"
            + "PROPERTIES (\n"
            + "\"replication_num\" = \"3\"\n"
            + "); ");
        CreateTable createTable = (CreateTable)node;
        boolean distributeKey = false;
        for (BaseConstraint baseConstraint : createTable.getConstraintStatements()) {
            if (baseConstraint instanceof DistributeConstraint) {
                DistributeConstraint uniqueConstraint = (DistributeConstraint)baseConstraint;
                distributeKey = true;
                assertEquals(uniqueConstraint.getCustomType(), DistributeConstraint.TYPE);
                assertEquals(1, uniqueConstraint.getColumns().size());
                assertEquals(Integer.valueOf(3), uniqueConstraint.getBucket());
            }
        }
        assertTrue(distributeKey);
    }

    @Test
    public void testParseWithDistributeBucketRandom() {
        Node node = starRocksLanguageParser.parseNode("CREATE TABLE IF NOT EXISTS orders (\n"
            + "    create_time DATE NOT NULL COMMENT \"create time of an order\",\n"
            + "    order_id BIGINT NOT NULL COMMENT \"id of an order\",\n"
            + "    order_state INT COMMENT \"state of an order\",\n"
            + "    total_price BIGINT COMMENT \"price of an order\"\n"
            + ")\n"
            + "UNIQUE KEY(create_time, order_id)\n"
            + "DISTRIBUTED BY random buckets 3\n"
            + "PROPERTIES (\n"
            + "\"replication_num\" = \"3\"\n"
            + "); ");
        CreateTable createTable = (CreateTable)node;
        boolean distributeKey = false;
        for (BaseConstraint baseConstraint : createTable.getConstraintStatements()) {
            if (baseConstraint instanceof DistributeConstraint) {
                DistributeConstraint uniqueConstraint = (DistributeConstraint)baseConstraint;
                distributeKey = true;
                assertEquals(uniqueConstraint.getCustomType(), DistributeConstraint.TYPE);
                assertEquals(Integer.valueOf(3), uniqueConstraint.getBucket());
                assertTrue(uniqueConstraint.getRandom());
            }
        }
        assertTrue(distributeKey);
    }

    /**
     * order by
     */
    @Test
    public void testParseWithOrderByModel() {
        Node node = starRocksLanguageParser.parseNode("create table users (\n"
            + "    user_id bigint NOT NULL,\n"
            + "    name string NOT NULL,\n"
            + "    email string NULL,\n"
            + "    address string NULL,\n"
            + "    age tinyint NULL,\n"
            + "    sex tinyint NULL,\n"
            + "    last_active datetime,\n"
            + "    property0 tinyint NOT NULL,\n"
            + "    property1 tinyint NOT NULL,\n"
            + "    property2 tinyint NOT NULL,\n"
            + "    property3 tinyint NOT NULL\n"
            + ") \n"
            + "PRIMARY KEY (`user_id`)\n"
            + "DISTRIBUTED BY HASH(`user_id`)\n"
            + "ORDER BY(`address`,`last_active`)\n"
            + "PROPERTIES(\n"
            + "    \"replication_num\" = \"3\",\n"
            + "    \"enable_persistent_index\" = \"true\"\n"
            + ");");
        CreateTable createTable = (CreateTable)node;
        boolean hasOrder = false;
        for (BaseConstraint baseConstraint : createTable.getConstraintStatements()) {
            if (baseConstraint instanceof OrderByConstraint) {
                OrderByConstraint orderByConstraint = (OrderByConstraint)baseConstraint;
                hasOrder = true;
                assertEquals(orderByConstraint.getCustomType(), OrderByConstraint.TYPE);
            }
        }
        assertTrue(hasOrder);
    }

    @Test
    public void testParseWithBitmap() {
        Node node1 = starRocksLanguageParser.parseNode("CREATE TABLE example_db.table_hash\n"
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
        CreateTable createTable = (CreateTable)node1;
        List<TableIndex> tableIndexList = createTable.getTableIndexList();
        assertEquals(1, tableIndexList.size());
        TableIndex tableIndex = tableIndexList.get(0);
        assertEquals("k1_idx", tableIndex.getIndexName().getValue());
        List<Property> properties = tableIndex.getProperties();
        assertEquals(2, properties.size());
    }

    @Test
    public void testParseWithCurrentTimestamp() {
        Node node = starRocksLanguageParser.parseNode("CREATE TABLE table_range\n"
            + "(\n"
            + "    k1 DATE,\n"
            + "    k2 INT,\n"
            + "    k3 SMALLINT,\n"
            + "    v1 VARCHAR(2048),\n"
            + "    v2 DATETIME DEFAULT CURRENT_TIMESTAMP\n"
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
        CreateTable createTable = (CreateTable)node;
        List<ColumnDefinition> columnDefines = createTable.getColumnDefines();
        ColumnDefinition v2 = columnDefines.stream().filter(x -> {
            return x.getColName().getValue().equalsIgnoreCase("v2");
        }).findFirst().get();
        BaseExpression defaultValue = v2.getDefaultValue();
        CurrentTimestamp currentTimestamp = (CurrentTimestamp)defaultValue;
        assertNotNull(currentTimestamp);
    }

    @Test
    public void testParseWithQualifiedName() {
        Node node = starRocksLanguageParser.parseNode("CREATE TABLE table_range\n"
            + "(\n"
            + "    k1 DATE,\n"
            + "    k2 INT,\n"
            + "    k3 SMALLINT,\n"
            + "    v1 VARCHAR(2048),\n"
            + "    v2 String DEFAULT (uuid())\n"
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
        CreateTable createTable = (CreateTable)node;
        List<ColumnDefinition> columnDefines = createTable.getColumnDefines();
        ColumnDefinition v2 = columnDefines.stream().filter(x -> {
            return x.getColName().getValue().equalsIgnoreCase("v2");
        }).findFirst().get();
        BaseExpression defaultValue = v2.getDefaultValue();
        assertEquals(FunctionCall.class, defaultValue.getClass());
    }

    @Test
    public void testDefaultInteger() {
        CreateTable o = starRocksLanguageParser.parseNode("CREATE TABLE site_access (\n"
            + "    datekey DATE,\n"
            + "    site_id INT,\n"
            + "    city_code SMALLINT,\n"
            + "    user_name VARCHAR(32),\n"
            + "    pv BIGINT DEFAULT '0'\n"
            + ")\n"
            + "ENGINE=olap\n"
            + "DUPLICATE KEY(datekey, site_id, city_code, user_name)\n"
            + "PARTITION BY RANGE (datekey) (\n"
            + "    START (\"2021-01-01\") END (\"2021-01-04\") EVERY (INTERVAL 1 DAY)\n"
            + ")\n"
            + "DISTRIBUTED BY HASH(site_id)\n"
            + "PROPERTIES (\n"
            + "    \"replication_num\" = \"3\" \n"
            + ");");
        assertNotNull(o);
    }

    @Test
    public void testParseError() {
        CreateTable o = starRocksLanguageParser.parseNode("CREATE TABLE site_access (\n"
            + "    datekey DATE,\n"
            + "    site_id INT,\n"
            + "    city_code SMALLINT,\n"
            + "    user_name VARCHAR(32),\n"
            + "    pv BIGINT DEFAULT '0'\n"
            + ")\n"
            + "ENGINE=olap\n"
            + "DUPLICATE KEY(datekey, site_id, city_code, user_name)\n"
            + "PARTITION BY RANGE (datekey) (\n"
            + "    START (\"2021-01-01\") END (\"2021-01-04\") EVERY (INTERVAL 1 DAY)\n"
            + ")\n"
            + "DISTRIBUTED BY RANDOM BUCKETS 5\n"
            + "PROPERTIES (\n"
            + "    \"replication_num\" = \"3\" \n"
            + ");");
        assertNotNull(o);
    }

    @Test
    public void testParseExpressionPartition() {
        CreateTable o = starRocksLanguageParser.parseNode("CREATE TABLE site_access (\n"
            + "    datekey DATE,\n"
            + "    site_id INT,\n"
            + "    city_code SMALLINT,\n"
            + "    user_name VARCHAR(32),\n"
            + "    pv BIGINT DEFAULT '0'\n"
            + ")\n"
            + "ENGINE=olap\n"
            + "DUPLICATE KEY(datekey, site_id, city_code, user_name)\n"
            + "PARTITION BY (datekey, site_id)"
            + "DISTRIBUTED BY RANDOM BUCKETS 5\n"
            + "PROPERTIES (\n"
            + "    \"replication_num\" = \"3\" \n"
            + ");");
        assertNotNull(o);
        PartitionedBy partitionedBy = o.getPartitionedBy();
        ExpressionPartitionBy expressionPartitionBy = (ExpressionPartitionBy)partitionedBy;
        assertNull(expressionPartitionBy.getFunctionCall());
        assertEquals(2, expressionPartitionBy.getColumnDefinitions().size());
    }
}
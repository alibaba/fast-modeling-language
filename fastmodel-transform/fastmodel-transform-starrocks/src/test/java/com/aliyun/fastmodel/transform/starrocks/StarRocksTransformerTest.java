package com.aliyun.fastmodel.transform.starrocks;

import java.util.List;
import java.util.Optional;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.transform.api.client.CodeGenerator;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.request.DdlGeneratorModelRequest;
import com.aliyun.fastmodel.transform.api.client.dto.result.DdlGeneratorResult;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.client.dto.table.TableConfig;
import com.aliyun.fastmodel.transform.api.client.generator.DefaultCodeGenerator;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.api.extension.client.constraint.DistributeClientConstraint;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.ColumnExpressionPartitionProperty;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.ListPartitionProperty;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.TablePartitionRaw;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.TimeExpressionPartitionProperty;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.ArrayClientPartitionKey;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.ColumnExpressionClientPartition;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.ListClientPartition;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.PartitionClientValue;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.TimeExpressionClientPartition;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksLanguageParser;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import static com.aliyun.fastmodel.core.formatter.ExpressionFormatter.formatExpression;
import static com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey.TABLE_ENGINE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2023/9/6
 */
public class StarRocksTransformerTest {

    StarRocksTransformer starRocksTransformer = new StarRocksTransformer();

    @Test
    public void transform() {
        CreateTable createTable = CreateTable
            .builder()
            .tableName(QualifiedName.of("ab"))
            .build();
        DialectNode transform = starRocksTransformer.transform(createTable);
        assertEquals("CREATE TABLE ab;", transform.getNode());
    }

    @Test
    public void testCreateProperty() {
        BaseClientProperty property = starRocksTransformer.create("external", "true");
        assertNotNull(property);
        assertEquals("external", property.getKey());
        assertEquals("true", property.getValue());
    }

    @Test
    public void testTransformTable() {
        DialectNode dialectNode = new DialectNode(
            "CREATE TABLE example_db.table_range\n"
                + "(\n"
                + "    k1 DATE,\n"
                + "    k2 ARRAY<INT>,\n"
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
                + ");"
        );
        Node node = starRocksTransformer.reverse(dialectNode);
        Table table = starRocksTransformer.transformTable(node, TransformContext.builder().build());
        assertNull(table.getSchema());
        assertEquals("example_db", table.getDatabase());
        assertEquals("table_range", table.getName());
        List<Column> columns = table.getColumns();
        assertEquals(5, columns.size());
        Column column = columns.get(1);
        assertEquals("ARRAY<INT>", column.getDataType());
        List<BaseClientProperty> properties = table.getProperties();
        assertEquals(6, properties.size());
        Optional<BaseClientProperty> first = properties.stream().filter(
            c -> StringUtils.equalsIgnoreCase(c.getKey(), TABLE_ENGINE.getValue())).findFirst();
        assertEquals(TABLE_ENGINE.getValue(), first.get().getKey());
    }

    @Test
    public void testTransformTableDistributedKey() {
        DialectNode dialectNode = new DialectNode(
            "CREATE TABLE example_db.table_hash\n" +
                "(\n" +
                "    k1 TINYINT AUTO_INCREMENT,\n" +
                "    k2 DECIMAL(10, 2) DEFAULT \"10.5\"\n" +
                ")\n" +
                "DISTRIBUTED BY RANDOM BUCKETS 56;"
        );
        Node node = starRocksTransformer.reverse(dialectNode);
        Table table = starRocksTransformer.transformTable(node, TransformContext.builder().build());
        assertNotNull(table.getConstraints());
        assertEquals(1, table.getConstraints().size());
        DistributeClientConstraint distributedKey = (DistributeClientConstraint)table.getConstraints().get(0);
        assertTrue(distributedKey.getRandom());
        assertEquals(56, distributedKey.getBucket().intValue());
    }

    @Test
    public void testTransformTableArray() {
        DialectNode dialectNode = new DialectNode(
            "CREATE TABLE example_db.table_range\n"
                + "(\n"
                + "    k1 DATE,\n"
                + "    k2 ARRAY<INT>,\n"
                + "    k3 SMALLINT,\n"
                + "    v1 VARCHAR(2048),\n"
                + "    v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n"
                + ")\n"
                + "ENGINE=olap\n"
                + "DUPLICATE KEY(k1, k2, k3)\n"
                + "PARTITION BY RANGE (k1)\n"
                + "(\n"
                + "    PARTITION p202101 VALUES [(\"20210101\"), (\"20210201\")),\n"
                + "    PARTITION p2 VALUES LESS THAN (\"2014-06-01\"),\n"
                + "    PARTITION p202103 VALUES [(\"20210301\"), (MAXVALUE))\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(k2) BUCKETS 10\n"
                + "PROPERTIES(\n"
                + "    \"storage_medium\" = \"SSD\", \n"
                + "    \"storage_cooldown_time\" = \"2015-06-04 00:00:00\"\n"
                + ");"
        );
        Node node = starRocksTransformer.reverse(dialectNode);
        Table table = starRocksTransformer.transformTable(node, TransformContext.builder().build());
        List<Column> columns = table.getColumns();
        assertEquals(5, columns.size());
        Column column = columns.get(1);
        assertEquals("ARRAY<INT>", column.getDataType());
        List<BaseClientProperty> properties = table.getProperties();
        assertEquals(6, properties.size());
        Optional<BaseClientProperty> first = properties.stream().filter(c -> {
            return StringUtils.equalsIgnoreCase(c.getKey(), TABLE_ENGINE.getValue());
        }).findFirst();
        assertEquals(TABLE_ENGINE.getValue(), first.get().getKey());
    }

    @Test
    public void testTransformWithoutPartition() {
        DialectNode dialectNode = new DialectNode(
            "CREATE TABLE IF NOT EXISTS ruoyun_db.fml_simple\n"
                + "(\n"
                + "   c1  TINYINT NOT NULL COMMENT \"ti_comment\",\n"
                + "   c2  SMALLINT NOT NULL COMMENT \"si_comment\",\n"
                + "   c3  STRING NULL\n"
                + ")\n"
                + "PRIMARY KEY (c1,c2)\n"
                + "COMMENT \"table_comment\"\n"
                + "DISTRIBUTED BY HASH(c1,c2) BUCKETS 4\n"
                + "PROPERTIES (\"replication_num\"=\"3\")"
        );
        Node node = starRocksTransformer.reverse(dialectNode);
        Table table = starRocksTransformer.transformTable(node, TransformContext.builder().build());
        List<Column> columns = table.getColumns();
        assertEquals(3, columns.size());
    }

    @Test
    public void testTransform() {
        DialectNode dialectNode = new DialectNode(
            "CREATE TABLE IF NOT EXISTS ruoyun_db.fml_lower_upper_bound_partition\n"
                + "(\n"
                + "   c1 TINYINT NOT NULL COMMENT \"c1 comment\",\n"
                + "   c2 DATE NOT NULL COMMENT \"c2 comment\",\n"
                + "   c3 INT NOT NULL COMMENT \"\",\n"
                + "   c4 STRING NULL COMMENT \"c4 comment\"\n"
                + ")\n"
                + "PRIMARY KEY (c1,c2,c3)\n"
                + "COMMENT \"table_comment\"\n"
                + "PARTITION BY RANGE (c3,c2)\n"
                + "(\n"
                + "   PARTITION IF NOT EXISTS pt1 VALUES [(\"1021-01-01\",\"2021-01-01\"),(\"1022-01-01\",\"2022-01-01\")),\n"
                + "   PARTITION IF NOT EXISTS pt2 VALUES [(\"1023-01-01\",\"2023-01-01\"),(\"1024-01-01\",\"2024-01-01\"))\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(c1,c2) BUCKETS 4\n"
                + "PROPERTIES (\"replication_num\"=\"3\");"
        );
        Node node = starRocksTransformer.reverse(dialectNode);
        Table table = starRocksTransformer.transformTable(node, TransformContext.builder().build());
        List<Column> columns = table.getColumns();
        Column column = columns.get(1);
        assertEquals(Integer.valueOf(1), column.getPartitionKeyIndex());
        column = columns.get(2);
        assertEquals(Integer.valueOf(0), column.getPartitionKeyIndex());
    }

    @Test
    public void testReversePartitionRaw() {
        DialectNode dialectNode = new DialectNode(
            "CREATE TABLE IF NOT EXISTS ruoyun_db.fml_raw_partition\n"
                + "(\n"
                + "   c1 TINYINT NOT NULL COMMENT \"c1 comment\",\n"
                + "   c2 DATE NOT NULL COMMENT \"c2 comment\",\n"
                + "   c3 INT NOT NULL COMMENT \"\",\n"
                + "   c4 STRING NULL COMMENT \"c4 comment\"\n"
                + ")\n"
                + "PRIMARY KEY (c1,c2,c3)\n"
                + "COMMENT \"table_comment\"\n"
                + "PARTITION BY RANGE (c2)()\n"
                + "DISTRIBUTED BY HASH(c1,c2) BUCKETS 4\n"
                + "PROPERTIES (\"replication_num\"=\"3\")"
        );
        Node node = starRocksTransformer.reverse(dialectNode);
        Table table = starRocksTransformer.transformTable(node, TransformContext.builder().build());
        List<BaseClientProperty> properties = table.getProperties();
        assertEquals(2, properties.size());
        Optional<BaseClientProperty> first = properties.stream().filter(p -> p instanceof TablePartitionRaw).findFirst();
        BaseClientProperty baseClientProperty = first.get();
        TablePartitionRaw tablePartitionRaw = (TablePartitionRaw)baseClientProperty;
        assertEquals("PARTITION BY RANGE (c2)()", tablePartitionRaw.getValue());
    }

    /**
     * 支持unique key的constraint
     */
    @Test
    public void testTransformUniqueKeyConstraint() {
        String ddl = "CREATE TABLE IF NOT EXISTS orders (\n"
            + "    create_time DATE NOT NULL COMMENT \"create time of an order\",\n"
            + "    order_id BIGINT NOT NULL COMMENT \"id of an order\",\n"
            + "    order_state INT COMMENT \"state of an order\",\n"
            + "    total_price BIGINT COMMENT \"price of an order\"\n"
            + ")\n"
            + "UNIQUE KEY(create_time, order_id)\n"
            + "DISTRIBUTED BY HASH(order_id)\n"
            + "PROPERTIES (\n"
            + "\"replication_num\" = \"3\"\n"
            + "); ";
        CreateTable node = (CreateTable)starRocksTransformer.reverse(new DialectNode(ddl));
        DialectNode transform = starRocksTransformer.transform(node);
        assertEquals("CREATE TABLE orders\n"
            + "(\n"
            + "   create_time DATE NOT NULL COMMENT \"create time of an order\",\n"
            + "   order_id    BIGINT NOT NULL COMMENT \"id of an order\",\n"
            + "   order_state INT COMMENT \"state of an order\",\n"
            + "   total_price BIGINT COMMENT \"price of an order\"\n"
            + ")\n"
            + "UNIQUE KEY (create_time,order_id)\n"
            + "DISTRIBUTED BY HASH(order_id)\n"
            + "PROPERTIES (\"replication_num\"=\"3\");", transform.getNode());
    }

    /**
     * 支持aggregate key的constraint
     */
    @Test
    public void testTransformAggregateConstraint() {
        String s = "create table users (\n"
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
            + ");";
        Node node = starRocksTransformer.reverse(new DialectNode(s));
        CreateTable createTable = (CreateTable)node;
        DialectNode transform = starRocksTransformer.transform(createTable);
        assertEquals("CREATE TABLE `users`\n"
            + "(\n"
            + "   user_id     BIGINT NOT NULL,\n"
            + "   `name`      STRING NOT NULL,\n"
            + "   email       STRING NULL,\n"
            + "   address     STRING NULL,\n"
            + "   age         TINYINT NULL,\n"
            + "   sex         TINYINT NULL,\n"
            + "   last_active DATETIME,\n"
            + "   property0   TINYINT NOT NULL,\n"
            + "   property1   TINYINT NOT NULL,\n"
            + "   property2   TINYINT NOT NULL,\n"
            + "   property3   TINYINT NOT NULL\n"
            + ")\n"
            + "PRIMARY KEY (`user_id`)\n"
            + "DISTRIBUTED BY HASH(`user_id`)\n"
            + "ORDER BY (`address`,`last_active`)\n"
            + "PROPERTIES (\"replication_num\"=\"3\",\"enable_persistent_index\"=\"true\");", transform.getNode());
    }

    @Test
    public void testTransformDuplicateConstraint() {
        String ddl = "CREATE TABLE IF NOT EXISTS detail (\n"
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
            + ");";
        Node node = starRocksTransformer.reverse(new DialectNode(ddl));
        CreateTable createTable = (CreateTable)node;
        DialectNode transform = starRocksTransformer.transform(createTable);
        assertEquals("CREATE TABLE detail\n"
            + "(\n"
            + "   event_time  DATETIME NOT NULL COMMENT \"datetime of event\",\n"
            + "   event_type  INT NOT NULL COMMENT \"type of event\",\n"
            + "   user_id     INT COMMENT \"id of user\",\n"
            + "   device_code INT COMMENT \"device code\",\n"
            + "   channel     INT COMMENT \"\"\n"
            + ")\n"
            + "DUPLICATE KEY (event_time,event_type)\n"
            + "DISTRIBUTED BY HASH(user_id)\n"
            + "PROPERTIES (\"replication_num\"=\"3\");", transform.getNode());
    }

    @Test
    public void testTransformOrderby() {
        String ddl = "create table users (\n"
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
            + ");";
        Node node = starRocksTransformer.reverse(new DialectNode(ddl));
        CreateTable createTable = (CreateTable)node;
        DialectNode transform = starRocksTransformer.transform(createTable);
        assertEquals("CREATE TABLE `users`\n"
            + "(\n"
            + "   user_id     BIGINT NOT NULL,\n"
            + "   `name`      STRING NOT NULL,\n"
            + "   email       STRING NULL,\n"
            + "   address     STRING NULL,\n"
            + "   age         TINYINT NULL,\n"
            + "   sex         TINYINT NULL,\n"
            + "   last_active DATETIME,\n"
            + "   property0   TINYINT NOT NULL,\n"
            + "   property1   TINYINT NOT NULL,\n"
            + "   property2   TINYINT NOT NULL,\n"
            + "   property3   TINYINT NOT NULL\n"
            + ")\n"
            + "PRIMARY KEY (`user_id`)\n"
            + "DISTRIBUTED BY HASH(`user_id`)\n"
            + "ORDER BY (`address`,`last_active`)\n"
            + "PROPERTIES (\"replication_num\"=\"3\",\"enable_persistent_index\"=\"true\");", transform.getNode());
    }

    @Test
    public void testTransformIndex() {
        String ddl = "CREATE TABLE example_db.table_hash\n"
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
            + "PROPERTIES (\"storage_type\"=\"column\");";
        CreateTable createTable = (CreateTable)starRocksTransformer.reverse(new DialectNode(ddl));
        DialectNode transform = starRocksTransformer.transform(createTable);
        assertEquals("CREATE TABLE example_db.table_hash\n"
            + "(\n"
            + "   k1 TINYINT,\n"
            + "   k2 DECIMAL(10,2) DEFAULT \"10.5\",\n"
            + "   v1 CHAR(10) REPLACE,\n"
            + "   v2 INT SUM,\n"
            + "   INDEX k1_idx (k1) USING BITMAP COMMENT \"xxxxxx\"\n"
            + ")\n"
            + "ENGINE=olap\n"
            + "AGGREGATE KEY (k1,k2)\n"
            + "COMMENT \"my first starrocks table\"\n"
            + "DISTRIBUTED BY HASH(k1) BUCKETS 10\n"
            + "PROPERTIES (\"storage_type\"=\"column\");", transform.getNode());
    }

    @Test
    public void testTransformRollup() {
        String ddl = "CREATE TABLE example_db.table_hash\n"
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
            + "ROLLUP (a (a, b) DUPLICATE KEY (b, c) FROM c PROPERTIES(\"a\"=\"d\"))"
            + "PROPERTIES (\"storage_type\"=\"column\");";
        CreateTable createTable = (CreateTable)starRocksTransformer.reverse(new DialectNode(ddl));
        DialectNode transform = starRocksTransformer.transform(createTable);
        assertEquals("CREATE TABLE example_db.table_hash\n"
            + "(\n"
            + "   k1 TINYINT,\n"
            + "   k2 DECIMAL(10,2) DEFAULT \"10.5\",\n"
            + "   v1 CHAR(10) REPLACE,\n"
            + "   v2 INT SUM,\n"
            + "   INDEX k1_idx (k1) USING BITMAP COMMENT \"xxxxxx\"\n"
            + ")\n"
            + "ENGINE=olap\n"
            + "AGGREGATE KEY (k1,k2)\n"
            + "COMMENT \"my first starrocks table\"\n"
            + "DISTRIBUTED BY HASH(k1) BUCKETS 10\n"
            + "ROLLUP (a (a,b) DUPLICATE KEY (b,c) FROM c PROPERTIES (\"a\"=\"d\"))\n"
            + "PROPERTIES (\"storage_type\"=\"column\");", transform.getNode());
    }

    @Test
    public void testTransformTable_Npe() {
        StarRocksLanguageParser starRocksLanguageParser = new StarRocksLanguageParser();
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
        Table table = starRocksTransformer.transformTable(o, TransformContext.builder().build());
        assertNotNull(table);
    }

    @Test
    public void testTransformListPartition() {
        {
            DialectNode dialectNode = new DialectNode(
                "CREATE TABLE t_recharge_detail2 (\n" +
                    "    id bigint,\n" +
                    "    city varchar(20) not null,\n" +
                    "    dt varchar(20) not null\n" +
                    ")\n" +
                    "PARTITION BY LIST (city) (\n" +
                    "   PARTITION pCalifornia VALUES IN (\"Los Angeles\",\"San Francisco\",\"San Diego\"),\n" +
                    "   PARTITION pTexas VALUES IN (\"Houston\",\"Dallas\",\"Austin\")\n" +
                    ");"
            );
            Node node = starRocksTransformer.reverse(dialectNode);
            Table table = starRocksTransformer.transformTable(node, TransformContext.builder().build());
            assertNull(table.getSchema());
            assertEquals("t_recharge_detail2", table.getName());

            assertNotNull(table.getProperties());
            assertEquals(2, table.getProperties().size());

            assertTrue(table.getProperties().get(0) instanceof ListPartitionProperty);
            ListPartitionProperty property = (ListPartitionProperty)table.getProperties().get(0);
            assertEquals("list_partition", property.getKey());
            ListClientPartition listClientPartition = property.getValue();
            assertEquals("pCalifornia", listClientPartition.getName());
            List<List<PartitionClientValue>> partitionValue =
                ((ArrayClientPartitionKey)listClientPartition.getPartitionKey()).getPartitionValue();

            assertEquals("[[PartitionClientValue(maxValue=false, value=Los Angeles)], " +
                "[PartitionClientValue(maxValue=false, value=San Francisco)], " +
                "[PartitionClientValue(maxValue=false, value=San Diego)]]", partitionValue.toString());
        }

        {
            DialectNode dialectNode = new DialectNode(
                "CREATE TABLE t_recharge_detail4 (\n" +
                    "    id bigint,\n" +
                    "    user_id bigint,\n" +
                    "    recharge_money decimal(32,2), \n" +
                    "    city varchar(20) not null,\n" +
                    "    dt varchar(20) not null\n" +
                    ")\n" +
                    "PARTITION BY LIST (dt,city) (\n" +
                    "   PARTITION p202204_California VALUES IN (\n" +
                    "       (\"2022-04-01\", \"Los Angeles\"),\n" +
                    "       (\"2022-04-01\", \"San Francisco\"),\n" +
                    "       (\"2022-04-02\", \"Los Angeles\"),\n" +
                    "       (\"2022-04-02\", \"San Francisco\")\n" +
                    "    ),\n" +
                    "   PARTITION p202204_Texas VALUES IN (\n" +
                    "       (\"2022-04-01\", \"Houston\"),\n" +
                    "       (\"2022-04-01\", \"Dallas\"),\n" +
                    "       (\"2022-04-02\", \"Houston\"),\n" +
                    "       (\"2022-04-02\", \"Dallas\")\n" +
                    "   )\n" +
                    ");"
            );
            Node node = starRocksTransformer.reverse(dialectNode);
            Table table = starRocksTransformer.transformTable(node, TransformContext.builder().build());

            assertNotNull(table.getProperties());
            assertEquals(2, table.getProperties().size());

            assertTrue(table.getProperties().get(0) instanceof ListPartitionProperty);
            ListPartitionProperty property = (ListPartitionProperty)table.getProperties().get(0);
            assertEquals("list_partition", property.getKey());
            ListClientPartition listClientPartition = property.getValue();
            assertEquals("p202204_California", listClientPartition.getName());
            List<List<PartitionClientValue>> partitionValue =
                ((ArrayClientPartitionKey)listClientPartition.getPartitionKey()).getPartitionValue();

            assertEquals("[[PartitionClientValue(maxValue=false, value=2022-04-01), " +
                "PartitionClientValue(maxValue=false, value=Los Angeles)], " +
                "[PartitionClientValue(maxValue=false, value=2022-04-01), " +
                "PartitionClientValue(maxValue=false, value=San Francisco)], " +
                "[PartitionClientValue(maxValue=false, value=2022-04-02), " +
                "PartitionClientValue(maxValue=false, value=Los Angeles)], " +
                "[PartitionClientValue(maxValue=false, value=2022-04-02), " +
                "PartitionClientValue(maxValue=false, value=San Francisco)]]", partitionValue.toString());
        }
    }

    @Test
    public void testReverseListPartition() {
        {
            DialectNode dialectNode = new DialectNode(
                "CREATE TABLE t_recharge_detail2 (\n" +
                    "    id bigint,\n" +
                    "    city varchar(20) not null,\n" +
                    "    dt varchar(20) not null\n" +
                    ")\n" +
                    "PARTITION BY LIST (city) (\n" +
                    "   PARTITION pCalifornia VALUES IN (\"Los Angeles\",\"San Francisco\",\"San Diego\"),\n" +
                    "   PARTITION pTexas VALUES IN (\"Houston\",\"Dallas\",\"Austin\")\n" +
                    ");"
            );
            Node node = starRocksTransformer.reverse(dialectNode);
            Table table = starRocksTransformer.transformTable(node, TransformContext.builder().build());

            DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
                .after(table)
                .config(TableConfig.builder()
                    .dialectMeta(DialectMeta.DEFAULT_STARROCKS)
                    .build())
                .build();
            CodeGenerator codeGenerator = new DefaultCodeGenerator();
            DdlGeneratorResult generate = codeGenerator.generate(request);
            List<DialectNode> dialectNodes = generate.getDialectNodes();
            assertEquals("CREATE TABLE t_recharge_detail2\n" +
                "(\n" +
                "   id   BIGINT NULL,\n" +
                "   city VARCHAR(20) NOT NULL,\n" +
                "   dt   VARCHAR(20) NOT NULL\n" +
                ")\n" +
                "PARTITION BY LIST (city)\n" +
                "(\n" +
                "PARTITION pCalifornia VALUES IN (\"Los Angeles\",\"San Francisco\",\"San Diego\"),\n" +
                "PARTITION pTexas VALUES IN (\"Houston\",\"Dallas\",\"Austin\")\n" +
                ");", dialectNodes.get(0).getNode());
        }

        {
            DialectNode dialectNode = new DialectNode(
                "CREATE TABLE t_recharge_detail4 (\n" +
                    "    id bigint,\n" +
                    "    user_id bigint,\n" +
                    "    recharge_money decimal(32,2), \n" +
                    "    city varchar(20) not null,\n" +
                    "    dt varchar(20) not null\n" +
                    ")\n" +
                    "PARTITION BY LIST (dt,city) (\n" +
                    "   PARTITION p202204_California VALUES IN (\n" +
                    "       (\"2022-04-01\", \"Los Angeles\"),\n" +
                    "       (\"2022-04-01\", \"San Francisco\"),\n" +
                    "       (\"2022-04-02\", \"Los Angeles\"),\n" +
                    "       (\"2022-04-02\", \"San Francisco\")\n" +
                    "    ),\n" +
                    "   PARTITION p202204_Texas VALUES IN (\n" +
                    "       (\"2022-04-01\", \"Houston\"),\n" +
                    "       (\"2022-04-01\", \"Dallas\"),\n" +
                    "       (\"2022-04-02\", \"Houston\"),\n" +
                    "       (\"2022-04-02\", \"Dallas\")\n" +
                    "   )\n" +
                    ");"
            );
            Node node = starRocksTransformer.reverse(dialectNode);
            Table table = starRocksTransformer.transformTable(node, TransformContext.builder().build());

            DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
                .after(table)
                .config(TableConfig.builder()
                    .dialectMeta(DialectMeta.DEFAULT_STARROCKS)
                    .build())
                .build();
            CodeGenerator codeGenerator = new DefaultCodeGenerator();
            DdlGeneratorResult generate = codeGenerator.generate(request);
            List<DialectNode> dialectNodes = generate.getDialectNodes();
            assertEquals("CREATE TABLE t_recharge_detail4\n" +
                "(\n" +
                "   id             BIGINT NULL,\n" +
                "   user_id        BIGINT NULL,\n" +
                "   recharge_money DECIMAL(32,2) NULL,\n" +
                "   city           VARCHAR(20) NOT NULL,\n" +
                "   dt             VARCHAR(20) NOT NULL\n" +
                ")\n" +
                "PARTITION BY LIST (dt,city)\n" +
                "(\n" +
                "PARTITION p202204_California VALUES IN ((\"2022-04-01\",\"Los Angeles\"),(\"2022-04-01\",\"San Francisco\"),(\"2022-04-02\",\"Los "
                + "Angeles\"),(\"2022-04-02\",\"San Francisco\")),\n"
                +
                "PARTITION p202204_Texas VALUES IN ((\"2022-04-01\",\"Houston\"),(\"2022-04-01\",\"Dallas\"),(\"2022-04-02\",\"Houston\"),"
                + "(\"2022-04-02\",\"Dallas\"))\n"
                +
                ");", dialectNodes.get(0).getNode());
        }
    }

    @Test
    public void testTransformExpressionPartition() {
        {
            DialectNode dialectNode = new DialectNode(
                "CREATE TABLE site_access1 (\n" +
                    "    event_day DATETIME NOT NULL,\n" +
                    "    site_id INT DEFAULT '10',\n" +
                    "    city_code VARCHAR(100),\n" +
                    "    user_name VARCHAR(32) DEFAULT '',\n" +
                    "    pv BIGINT DEFAULT '0'\n" +
                    ")\n" +
                    "PARTITION BY date_trunc('day', event_day);"
            );
            Node node = starRocksTransformer.reverse(dialectNode);
            Table table = starRocksTransformer.transformTable(node, TransformContext.builder().build());

            assertEquals(true, table.getColumns().get(0).isPartitionKey());

            assertNotNull(table.getProperties());
            assertEquals(1, table.getProperties().size());

            assertTrue(table.getProperties().get(0) instanceof TimeExpressionPartitionProperty);
            TimeExpressionPartitionProperty property = (TimeExpressionPartitionProperty)table.getProperties().get(0);
            assertEquals("expression_partition", property.getKey());
            TimeExpressionClientPartition listClientPartition = property.getValue();
            assertEquals("date_trunc", listClientPartition.getFuncName());
            assertEquals("day", listClientPartition.getTimeUnit());
        }

        {
            DialectNode dialectNode = new DialectNode(
                "CREATE TABLE site_access3 (\n" +
                    "    event_day DATETIME NOT NULL,\n" +
                    "    site_id INT DEFAULT '10',\n" +
                    "    city_code VARCHAR(100),\n" +
                    "    user_name VARCHAR(32) DEFAULT '',\n" +
                    "    pv BIGINT DEFAULT '0'\n" +
                    ")\n" +
                    "PARTITION BY time_slice(event_day, INTERVAL 7 day);"
            );
            Node node = starRocksTransformer.reverse(dialectNode);
            Table table = starRocksTransformer.transformTable(node, TransformContext.builder().build());

            assertEquals(true, table.getColumns().get(0).isPartitionKey());

            assertNotNull(table.getProperties());
            assertEquals(1, table.getProperties().size());

            assertTrue(table.getProperties().get(0) instanceof TimeExpressionPartitionProperty);
            TimeExpressionPartitionProperty property = (TimeExpressionPartitionProperty)table.getProperties().get(0);
            assertEquals("expression_partition", property.getKey());
            TimeExpressionClientPartition listClientPartition = property.getValue();
            assertEquals("time_slice", listClientPartition.getFuncName());
            assertEquals("INTERVAL 7 DAY", formatExpression(listClientPartition.getInterval()));
        }

        {
            DialectNode dialectNode = new DialectNode(
                "CREATE TABLE t_recharge_detail1 (\n" +
                    "    id bigint,\n" +
                    "    user_id bigint,\n" +
                    "    recharge_money decimal(32,2), \n" +
                    "    city varchar(20) not null,\n" +
                    "    dt varchar(20) not null\n" +
                    ")\n" +
                    "PARTITION BY (dt,city);"
            );
            Node node = starRocksTransformer.reverse(dialectNode);
            Table table = starRocksTransformer.transformTable(node, TransformContext.builder().build());

            assertEquals(true, table.getColumns().get(3).isPartitionKey());
            assertEquals(true, table.getColumns().get(4).isPartitionKey());

            assertNotNull(table.getProperties());
            assertEquals(1, table.getProperties().size());

            assertTrue(table.getProperties().get(0) instanceof ColumnExpressionPartitionProperty);
            ColumnExpressionPartitionProperty property = (ColumnExpressionPartitionProperty)table.getProperties().get(0);
            ColumnExpressionClientPartition listClientPartition = property.getValue();
            assertEquals("[dt, city]", listClientPartition.getColumnNameList().toString());
        }
    }

    @Test
    public void testReverseExpressionPartition() {
        {
            DialectNode dialectNode = new DialectNode(
                "CREATE TABLE site_access1 (\n" +
                    "    event_day DATETIME NOT NULL,\n" +
                    "    site_id INT DEFAULT '10',\n" +
                    "    city_code VARCHAR(100),\n" +
                    "    user_name VARCHAR(32) DEFAULT '',\n" +
                    "    pv BIGINT DEFAULT '0'\n" +
                    ")\n" +
                    "PARTITION BY date_trunc('day', event_day);"
            );
            Node node = starRocksTransformer.reverse(dialectNode);
            Table table = starRocksTransformer.transformTable(node, TransformContext.builder().build());

            DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
                .after(table)
                .config(TableConfig.builder()
                    .dialectMeta(DialectMeta.DEFAULT_STARROCKS)
                    .build())
                .build();
            CodeGenerator codeGenerator = new DefaultCodeGenerator();
            DdlGeneratorResult generate = codeGenerator.generate(request);
            List<DialectNode> dialectNodes = generate.getDialectNodes();
            assertEquals("CREATE TABLE site_access1\n" +
                "(\n" +
                "   event_day DATETIME NOT NULL,\n" +
                "   site_id   INT NULL DEFAULT \"10\",\n" +
                "   city_code VARCHAR(100) NULL,\n" +
                "   user_name VARCHAR(32) NULL DEFAULT \"\",\n" +
                "   pv        BIGINT NULL DEFAULT \"0\"\n" +
                ")\n" +
                "PARTITION BY date_trunc('day', event_day);", dialectNodes.get(0).getNode());
        }

        {
            DialectNode dialectNode = new DialectNode(
                "CREATE TABLE site_access3 (\n" +
                    "    event_day DATETIME NOT NULL,\n" +
                    "    site_id INT DEFAULT '10',\n" +
                    "    city_code VARCHAR(100),\n" +
                    "    user_name VARCHAR(32) DEFAULT '',\n" +
                    "    pv BIGINT DEFAULT '0'\n" +
                    ")\n" +
                    "PARTITION BY time_slice(event_day, INTERVAL 7 day);"
            );
            Node node = starRocksTransformer.reverse(dialectNode);
            Table table = starRocksTransformer.transformTable(node, TransformContext.builder().build());

            DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
                .after(table)
                .config(TableConfig.builder()
                    .dialectMeta(DialectMeta.DEFAULT_STARROCKS)
                    .build())
                .build();
            CodeGenerator codeGenerator = new DefaultCodeGenerator();
            DdlGeneratorResult generate = codeGenerator.generate(request);
            List<DialectNode> dialectNodes = generate.getDialectNodes();
            assertEquals("CREATE TABLE site_access3\n" +
                "(\n" +
                "   event_day DATETIME NOT NULL,\n" +
                "   site_id   INT NULL DEFAULT \"10\",\n" +
                "   city_code VARCHAR(100) NULL,\n" +
                "   user_name VARCHAR(32) NULL DEFAULT \"\",\n" +
                "   pv        BIGINT NULL DEFAULT \"0\"\n" +
                ")\n" +
                "PARTITION BY time_slice(event_day, INTERVAL 7 DAY);", dialectNodes.get(0).getNode());
        }

        {
            DialectNode dialectNode = new DialectNode(
                "CREATE TABLE t_recharge_detail1 (\n" +
                    "    id bigint,\n" +
                    "    user_id bigint,\n" +
                    "    recharge_money decimal(32,2), \n" +
                    "    city varchar(20) not null,\n" +
                    "    dt varchar(20) not null\n" +
                    ")\n" +
                    "PARTITION BY (dt,city);"
            );
            Node node = starRocksTransformer.reverse(dialectNode);
            Table table = starRocksTransformer.transformTable(node, TransformContext.builder().build());

            DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
                .after(table)
                .config(TableConfig.builder()
                    .dialectMeta(DialectMeta.DEFAULT_STARROCKS)
                    .build())
                .build();
            CodeGenerator codeGenerator = new DefaultCodeGenerator();
            DdlGeneratorResult generate = codeGenerator.generate(request);
            List<DialectNode> dialectNodes = generate.getDialectNodes();
            assertEquals("CREATE TABLE t_recharge_detail1\n" +
                "(\n" +
                "   id             BIGINT NULL,\n" +
                "   user_id        BIGINT NULL,\n" +
                "   recharge_money DECIMAL(32,2) NULL,\n" +
                "   city           VARCHAR(20) NOT NULL,\n" +
                "   dt             VARCHAR(20) NOT NULL\n" +
                ")\n" +
                "PARTITION BY (dt,city);", dialectNodes.get(0).getNode());
        }
    }

    @Test
    public void testReverse() {
        String node = "CREATE TABLE site_access (\n"
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
            + ");";
        BaseStatement reverse = starRocksTransformer.reverse(new DialectNode(node));
        assertNotNull(reverse);
    }
}
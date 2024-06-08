package com.aliyun.fastmodel.transform.starrocks;

import java.util.List;
import java.util.Optional;

import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.starrocks.client.property.table.TablePartitionRaw;
import com.aliyun.fastmodel.transform.starrocks.format.StarRocksProperty;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

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
        assertEquals("CREATE TABLE ab", transform.getNode());
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
        assertEquals(8, properties.size());
        Optional<BaseClientProperty> first = properties.stream().filter(
            c -> StringUtils.equalsIgnoreCase(c.getKey(), StarRocksProperty.TABLE_ENGINE.getValue())).findFirst();
        assertEquals(StarRocksProperty.TABLE_ENGINE.getValue(), first.get().getKey());
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
        assertEquals(8, properties.size());
        Optional<BaseClientProperty> first = properties.stream().filter(c -> {
            return StringUtils.equalsIgnoreCase(c.getKey(), StarRocksProperty.TABLE_ENGINE.getValue());
        }).findFirst();
        assertEquals(StarRocksProperty.TABLE_ENGINE.getValue(), first.get().getKey());
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
        assertEquals(4, properties.size());
        Optional<BaseClientProperty> first = properties.stream().filter(p -> {
            return p instanceof TablePartitionRaw;
        }).findFirst();
        BaseClientProperty baseClientProperty = first.get();
        TablePartitionRaw tablePartitionRaw = (TablePartitionRaw)baseClientProperty;
        assertEquals("PARTITION BY RANGE (c2)()", tablePartitionRaw.getValue());
    }
}
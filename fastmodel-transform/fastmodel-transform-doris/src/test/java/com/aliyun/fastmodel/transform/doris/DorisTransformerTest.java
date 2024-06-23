package com.aliyun.fastmodel.transform.doris;

import java.nio.charset.Charset;
import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.google.common.collect.Lists;
import lombok.SneakyThrows;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * DorisTransformerTest
 *
 * @author panguanjing
 * @date 2024/1/20
 */
public class DorisTransformerTest {

    DorisTransformer dorisTransformer = new DorisTransformer();

    @Test
    public void reverse() {
        DialectNode dialectNode = new DialectNode("create table d_1 (a int);");
        ReverseContext context = ReverseContext.builder().merge(true).build();
        BaseStatement reverse = dorisTransformer.reverse(dialectNode, context);
        assertNotNull(reverse);
    }

    @Test
    @SneakyThrows
    public void testReverseRange() {
        String rangeContent = IOUtils.resourceToString("/doris/range.txt", Charset.defaultCharset());
        DialectNode dialectNode = new DialectNode(rangeContent);
        BaseStatement reverse = dorisTransformer.reverse(dialectNode);
        assertNotNull(reverse);
        CreateTable createTable = (CreateTable)reverse;
        List<ColumnDefinition> columnDefines = createTable.getColumnDefines();
        assertEquals(10, columnDefines.size());
    }

    @Test
    @SneakyThrows
    public void transform() {
        String rangeContent = IOUtils.resourceToString("/doris/range.txt", Charset.defaultCharset());
        DialectNode dialectNode = new DialectNode(rangeContent);
        BaseStatement reverse = dorisTransformer.reverse(dialectNode);
        assertNotNull(reverse);
        DialectNode transform = dorisTransformer.transform(reverse);
        assertEquals("CREATE TABLE example_db.example_range_tbl\n"
                + "(\n"
                + "   `user_id`         LARGEINT NOT NULL COMMENT \"User ID\",\n"
                + "   `date`            DATE NOT NULL COMMENT \"Date when the data are imported\",\n"
                + "   `timestamp`       DATETIME NOT NULL COMMENT \"Timestamp when the data are imported\",\n"
                + "   `city`            VARCHAR(20) NULL COMMENT \"User location city\",\n"
                + "   `age`             SMALLINT NULL COMMENT \"User age\",\n"
                + "   `sex`             TINYINT NULL COMMENT \"User gender\",\n"
                + "   `last_visit_date` DATETIME REPLACE NULL DEFAULT \"1970-01-01 00:00:00\" COMMENT \"User last visit time\",\n"
                + "   `cost`            BIGINT SUM NULL DEFAULT \"0\" COMMENT \"Total user consumption\",\n"
                + "   `max_dwell_time`  INT MAX NULL DEFAULT \"0\" COMMENT \"Maximum user dwell time\",\n"
                + "   `min_dwell_time`  INT MIN NULL DEFAULT \"99999\" COMMENT \"Minimum user dwell time\"\n"
                + ")\n"
                + "ENGINE=olap\n"
                + "AGGREGATE KEY (`user_id`,`date`,`timestamp`,`city`,`age`,`sex`)\n"
                + "PARTITION BY RANGE (`date`)\n"
                + "(\n"
                + "   PARTITION `p201701` VALUES LESS THAN (\"2017-02-01\"),\n"
                + "   PARTITION `p201702` VALUES LESS THAN (\"2017-03-01\"),\n"
                + "   PARTITION `p201703` VALUES LESS THAN (\"2017-04-01\")\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(`user_id`) BUCKETS 16\n"
                + "PROPERTIES (\"replication_num\"=\"3\",\"storage_medium\"=\"SSD\",\"storage_cooldown_time\"=\"2018-01-01 12:00:00\");",
            transform.getNode());
    }

    @Test
    @SneakyThrows
    public void reverseWithProperty() {
        String table = IOUtils.resourceToString("/doris/property.txt", Charset.defaultCharset());
        DialectNode dialectNode = new DialectNode(table);
        CreateTable reverse = (CreateTable)dorisTransformer.reverse(dialectNode);
        List<Property> properties = reverse.getProperties();
        assertTrue(properties.size() > 0);
        DialectNode transform = dorisTransformer.transform(reverse);
        assertEquals("CREATE TABLE `t_yunshi_holo_binlog_2_020601`\n"
                + "(\n"
                + "   `id`   BIGINT(20) NOT NULL,\n"
                + "   `col1` TEXT NULL,\n"
                + "   `col2` TEXT NULL,\n"
                + "   `col3` TEXT NULL\n"
                + ")\n"
                + "ENGINE=OLAP\n"
                + "UNIQUE KEY (`id`)\n"
                + "COMMENT \"Auto created by DataWorks Data Integration\"\n"
                + "DISTRIBUTED BY HASH(`id`) BUCKETS 4\n"
                + "PROPERTIES (\"replication_allocation\"=\"tag.location.default: 1\",\"in_memory\"=\"false\",\"storage_format\"=\"V2\","
                + "\"light_schema_change\"=\"true\",\"disable_auto_compaction\"=\"false\");",
            transform.getNode());
    }

    @Test
    @SneakyThrows
    public void reverseTable() {
        List<Column> columns = Lists.newArrayList();
        Column e = Column.builder()
            .name("c1")
            .comment("comment")
            .dataType("bigint")
            .build();
        columns.add(e);
        Table table = Table.builder()
            .name("t1")
            .columns(columns)
            .build();
        Node node = dorisTransformer.reverseTable(table);
        DialectNode transform = dorisTransformer.transform((BaseStatement)node);
        assertEquals("CREATE TABLE IF NOT EXISTS t1\n"
            + "(\n"
            + "   c1 BIGINT NOT NULL COMMENT \"comment\"\n"
            + ");", transform.getNode());

    }

    @Test
    public void transformTable() {
        List<ColumnDefinition> columns = Lists.newArrayList();
        ColumnDefinition e = ColumnDefinition.builder()
            .colName(new Identifier("c1"))
            .dataType(DataTypeUtil.simpleType("bigint", null))
            .build();
        columns.add(e);
        Node table = CreateTable.builder()
            .tableName(QualifiedName.of("t1"))
            .columns(columns)
            .build();
        Table table1 = dorisTransformer.transformTable(table, TransformContext.builder().build());
        assertEquals("t1", table1.getName());
        List<Column> columns1 = table1.getColumns();
        assertEquals(1, columns1.size());
    }

}
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

package com.aliyun.fastmodel.transform.clickhouse;

import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.clickhouse.format.ClickHousePropertyKey;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * ClickHouse 官方开源文档/issue 里的整句建表样例，作为回归基线
 * <p>
 * 样例来源：
 * 1. ReplacingMergeTree 官方示例（clickhouse-docs: managing-data/updating-data/overview.mdx）
 * 2. 官方分片部署示例（clickhouse-docs: deployment-guides/replication-sharding-examples/02_2_shards_1_replica.md）
 * 3. ClickHouse 仓库 issue #44621 复现语句
 *
 * @author panguanjing
 */
public class ClickHouseGitHubBaselineTest {

    private final ClickHouseTransformer transformer = new ClickHouseTransformer();

    private final TransformContext context = TransformContext.builder().build();

    private String regenerate(String ddl) {
        BaseStatement statement = transformer.reverse(new DialectNode(ddl));
        return transformer.transform(statement, context).getNode();
    }

    private CreateTable parse(String ddl) {
        return (CreateTable)transformer.reverse(new DialectNode(ddl));
    }

    private String getProperty(List<Property> properties, ClickHousePropertyKey propertyKey) {
        return properties.stream().filter(p -> propertyKey.getValue().equals(p.getName()))
            .findFirst().map(Property::getValue).orElse(null);
    }

    @Test
    public void testReplacingMergeTreeOfficialSample() {
        String ddl = "CREATE TABLE posts\n"
            + "(\n"
            + "    Id UInt32,\n"
            + "    Title String,\n"
            + "    ViewCount UInt32,\n"
            + "    Version UInt32\n"
            + ") ENGINE = ReplacingMergeTree(Version)\n"
            + "ORDER BY Id";
        String expected = "CREATE TABLE posts\n"
            + "(\n"
            + "   Id        UInt32,\n"
            + "   Title     String,\n"
            + "   ViewCount UInt32,\n"
            + "   Version   UInt32\n"
            + ")\n"
            + "ENGINE = ReplacingMergeTree(Version)\n"
            + "ORDER BY Id;";
        assertEquals(expected, regenerate(ddl));
        //二次往返保持稳定
        assertEquals(expected, regenerate(expected));

        CreateTable createTable = parse(ddl);
        assertEquals("ReplacingMergeTree(Version)",
            getProperty(createTable.getProperties(), ClickHousePropertyKey.TABLE_ENGINE));
        assertEquals("Id", getProperty(createTable.getProperties(), ClickHousePropertyKey.TABLE_ORDER_BY));
        //ClickHouse 标识符大小写敏感，列名不应被改写
        assertEquals("Id", createTable.getColumnDefines().get(0).getColName().getValue());
        assertEquals("ViewCount", createTable.getColumnDefines().get(2).getColName().getValue());

        Table table = transformer.transformTable(createTable, context);
        assertEquals("posts", table.getName());
        assertEquals("Version", table.getColumns().get(3).getName());
        assertEquals("UInt32", table.getColumns().get(3).getDataType());
    }

    @Test
    public void testShardingDeploymentOfficialSample() {
        String ddl = "CREATE TABLE test.test_table ON CLUSTER cluster_2S_1R\n"
            + "(\n"
            + "    `id` UInt64,\n"
            + "    `name` String\n"
            + ") ENGINE = MergeTree()\n"
            + "ORDER BY id";
        String expected = "CREATE TABLE test.test_table ON CLUSTER cluster_2S_1R\n"
            + "(\n"
            + "   `id`   UInt64,\n"
            + "   `name` String\n"
            + ")\n"
            + "ENGINE = MergeTree()\n"
            + "ORDER BY id;";
        assertEquals(expected, regenerate(ddl));
        assertEquals(expected, regenerate(expected));

        CreateTable createTable = parse(ddl);
        assertEquals("cluster_2S_1R",
            getProperty(createTable.getProperties(), ClickHousePropertyKey.TABLE_CLUSTER));
        assertEquals("MergeTree()", getProperty(createTable.getProperties(), ClickHousePropertyKey.TABLE_ENGINE));
        assertEquals("id", getProperty(createTable.getProperties(), ClickHousePropertyKey.TABLE_ORDER_BY));

        Table table = transformer.transformTable(createTable, context);
        assertEquals("test", table.getDatabase());
        assertEquals("test_table", table.getName());
        List<Column> columns = table.getColumns();
        assertEquals(2, columns.size());
        //反引号是引用形态，列名本身的取值必须保真
        assertEquals("id", columns.get(0).getName());
        assertEquals("UInt64", columns.get(0).getDataType());
        assertEquals("name", columns.get(1).getName());
        assertEquals("String", columns.get(1).getDataType());
    }

    @Test
    public void testReplicatedMergeTreeIssueSample() {
        String ddl = "CREATE TABLE db1.events ON CLUSTER 'ch-pro-cluster'\n"
            + "(\n"
            + "    ID Int64,\n"
            + "    x01 String,\n"
            + "    x02 String\n"
            + ") ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/dd', '{replica}')\n"
            + "ORDER BY ID\n"
            + "PARTITION BY ID % 5\n"
            + "SETTINGS min_rows_for_wide_part = 10, min_bytes_for_wide_part = 1";
        String expected = "CREATE TABLE db1.events ON CLUSTER 'ch-pro-cluster'\n"
            + "(\n"
            + "   ID  Int64,\n"
            + "   x01 String,\n"
            + "   x02 String\n"
            + ")\n"
            + "ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/dd', '{replica}')\n"
            + "PARTITION BY ID % 5\n"
            + "ORDER BY ID\n"
            + "SETTINGS min_rows_for_wide_part = 10, min_bytes_for_wide_part = 1;";
        assertEquals(expected, regenerate(ddl));
        assertEquals(expected, regenerate(expected));

        CreateTable createTable = parse(ddl);
        assertEquals("'ch-pro-cluster'",
            getProperty(createTable.getProperties(), ClickHousePropertyKey.TABLE_CLUSTER));
        assertEquals("ReplicatedMergeTree('/clickhouse/tables/{shard}/dd', '{replica}')",
            getProperty(createTable.getProperties(), ClickHousePropertyKey.TABLE_ENGINE));
        assertEquals("ID % 5", getProperty(createTable.getProperties(), ClickHousePropertyKey.TABLE_PARTITION_BY));
        assertEquals("ID", getProperty(createTable.getProperties(), ClickHousePropertyKey.TABLE_ORDER_BY));
        assertEquals("min_rows_for_wide_part = 10, min_bytes_for_wide_part = 1",
            getProperty(createTable.getProperties(), ClickHousePropertyKey.TABLE_SETTINGS));

        Table table = transformer.transformTable(createTable, context);
        assertEquals("db1", table.getDatabase());
        assertEquals("events", table.getName());
        assertEquals("ID", table.getColumns().get(0).getName());
        assertEquals("Int64", table.getColumns().get(0).getDataType());
    }

    @Test
    public void testBaselineSamplesAreParseableAfterTransform() {
        String[] samples = new String[] {
            "CREATE TABLE posts (Id UInt32, Title String, ViewCount UInt32, Version UInt32) "
                + "ENGINE = ReplacingMergeTree(Version) ORDER BY Id",
            "CREATE TABLE test.test_table ON CLUSTER cluster_2S_1R (`id` UInt64, `name` String) "
                + "ENGINE = MergeTree() ORDER BY id",
            "CREATE TABLE db1.events ON CLUSTER 'ch-pro-cluster' (ID Int64, x01 String, x02 String) "
                + "ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/dd', '{replica}') "
                + "ORDER BY ID PARTITION BY ID % 5 "
                + "SETTINGS min_rows_for_wide_part = 10, min_bytes_for_wide_part = 1",
        };
        for (String sample : samples) {
            String generated = regenerate(sample);
            assertNotNull(transformer.reverse(new DialectNode(generated)));
            assertTrue(generated.endsWith(";"));
        }
    }
}

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

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.clickhouse.parser.tree.ClickHouseAlterTable;
import com.aliyun.fastmodel.transform.clickhouse.parser.tree.ClickHouseAlterTableClause.ClauseType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * ALTER TABLE 分区运维语句的解析与再生成测试
 *
 * @author panguanjing
 */
public class ClickHouseAlterPartitionTest {

    private final ClickHouseTransformer transformer = new ClickHouseTransformer();

    private final TransformContext context = TransformContext.builder().build();

    private ClickHouseAlterTable reverse(String ddl) {
        BaseStatement statement = transformer.reverse(new DialectNode(ddl));
        assertTrue("期望解析为 ALTER 语句，实际:" + statement.getClass().getSimpleName(),
            statement instanceof ClickHouseAlterTable);
        return (ClickHouseAlterTable)statement;
    }

    private String regenerate(String ddl) {
        return transformer.transform(reverse(ddl), context).getNode();
    }

    @Test
    public void testDropPartition() {
        String ddl = "ALTER TABLE db.t DROP PARTITION '202401'";
        ClickHouseAlterTable statement = reverse(ddl);
        assertEquals("db.t", statement.getTableName().toString());
        assertEquals(1, statement.getClauses().size());
        assertEquals(ClauseType.DROP_PARTITION, statement.getClauses().get(0).getType());
        assertEquals("'202401'", statement.getClauses().get(0).getPartition());
        assertEquals("ALTER TABLE db.t DROP PARTITION '202401';", regenerate(ddl));
    }

    @Test
    public void testDropPartitionById() {
        String ddl = "ALTER TABLE db.t DROP PARTITION ID '202401'";
        assertEquals("ID '202401'", reverse(ddl).getClauses().get(0).getPartition());
        assertEquals("ALTER TABLE db.t DROP PARTITION ID '202401';", regenerate(ddl));
    }

    @Test
    public void testDropPartitionExpression() {
        String ddl = "ALTER TABLE db.t DROP PARTITION toYYYYMM(toDate('2024-01-01'))";
        assertEquals("toYYYYMM(toDate('2024-01-01'))", reverse(ddl).getClauses().get(0).getPartition());
        assertEquals("ALTER TABLE db.t DROP PARTITION toYYYYMM(toDate('2024-01-01'));", regenerate(ddl));
    }

    @Test
    public void testDetachPartition() {
        String ddl = "ALTER TABLE db.t DETACH PARTITION '202401'";
        assertEquals(ClauseType.DETACH_PARTITION, reverse(ddl).getClauses().get(0).getType());
        assertEquals("ALTER TABLE db.t DETACH PARTITION '202401';", regenerate(ddl));
    }

    @Test
    public void testAttachPartition() {
        String ddl = "ALTER TABLE db.t ATTACH PARTITION '202401'";
        assertNull(reverse(ddl).getClauses().get(0).getFromTable());
        assertEquals("ALTER TABLE db.t ATTACH PARTITION '202401';", regenerate(ddl));
    }

    @Test
    public void testAttachPartitionFrom() {
        String ddl = "ALTER TABLE db.t ATTACH PARTITION '202401' FROM db.t_backup";
        assertEquals("db.t_backup", reverse(ddl).getClauses().get(0).getFromTable());
        assertEquals("ALTER TABLE db.t ATTACH PARTITION '202401' FROM db.t_backup;", regenerate(ddl));
    }

    @Test
    public void testFreeze() {
        assertEquals("ALTER TABLE db.t FREEZE;", regenerate("ALTER TABLE db.t FREEZE"));
        assertNull(reverse("ALTER TABLE db.t FREEZE").getClauses().get(0).getPartition());
    }

    @Test
    public void testFreezePartition() {
        assertEquals("ALTER TABLE db.t FREEZE PARTITION '202401';",
            regenerate("ALTER TABLE db.t FREEZE PARTITION '202401'"));
    }

    @Test
    public void testMovePartitionToDisk() {
        String ddl = "ALTER TABLE db.t MOVE PARTITION '202401' TO DISK 'cold'";
        assertEquals(ClauseType.MOVE_PARTITION_TO_DISK, reverse(ddl).getClauses().get(0).getType());
        assertEquals("'cold'", reverse(ddl).getClauses().get(0).getTarget());
        assertEquals("ALTER TABLE db.t MOVE PARTITION '202401' TO DISK 'cold';", regenerate(ddl));
    }

    @Test
    public void testMovePartitionToVolume() {
        String ddl = "ALTER TABLE db.t MOVE PARTITION '202401' TO VOLUME 'v1'";
        assertEquals(ClauseType.MOVE_PARTITION_TO_VOLUME, reverse(ddl).getClauses().get(0).getType());
        assertEquals("ALTER TABLE db.t MOVE PARTITION '202401' TO VOLUME 'v1';", regenerate(ddl));
    }

    @Test
    public void testMovePartitionToTable() {
        String ddl = "ALTER TABLE db.t MOVE PARTITION '202401' TO TABLE db.t2";
        assertEquals(ClauseType.MOVE_PARTITION_TO_TABLE, reverse(ddl).getClauses().get(0).getType());
        assertEquals("db.t2", reverse(ddl).getClauses().get(0).getTarget());
        assertEquals("ALTER TABLE db.t MOVE PARTITION '202401' TO TABLE db.t2;", regenerate(ddl));
    }

    @Test
    public void testReplacePartition() {
        String ddl = "ALTER TABLE db.t REPLACE PARTITION '202401' FROM db.t2";
        assertEquals(ClauseType.REPLACE_PARTITION, reverse(ddl).getClauses().get(0).getType());
        assertEquals("db.t2", reverse(ddl).getClauses().get(0).getFromTable());
        assertEquals("ALTER TABLE db.t REPLACE PARTITION '202401' FROM db.t2;", regenerate(ddl));
    }

    @Test
    public void testOnCluster() {
        String ddl = "ALTER TABLE db.t ON CLUSTER cluster_2S_1R DROP PARTITION '202401'";
        assertEquals("cluster_2S_1R", reverse(ddl).getCluster());
        assertEquals("ALTER TABLE db.t ON CLUSTER cluster_2S_1R DROP PARTITION '202401';", regenerate(ddl));
    }

    @Test
    public void testMultipleClauses() {
        String ddl = "ALTER TABLE db.t DROP PARTITION '202401', DROP PARTITION '202402'";
        assertEquals(2, reverse(ddl).getClauses().size());
        assertEquals("ALTER TABLE db.t DROP PARTITION '202401', DROP PARTITION '202402';", regenerate(ddl));
    }

    @Test
    public void testRoundTripIsIdempotent() {
        String[] samples = new String[] {
            "ALTER TABLE db.t DROP PARTITION '202401';",
            "ALTER TABLE db.t ON CLUSTER c1 DETACH PARTITION ID '202401';",
            "ALTER TABLE db.t MOVE PARTITION '202401' TO DISK 'cold';",
            "ALTER TABLE db.t REPLACE PARTITION '202401' FROM db.t2;",
            "ALTER TABLE db.t ATTACH PARTITION '202401', FREEZE PARTITION '202402';",
        };
        for (String sample : samples) {
            assertEquals(sample, regenerate(sample));
            assertEquals(sample, regenerate(regenerate(sample)));
        }
    }
}

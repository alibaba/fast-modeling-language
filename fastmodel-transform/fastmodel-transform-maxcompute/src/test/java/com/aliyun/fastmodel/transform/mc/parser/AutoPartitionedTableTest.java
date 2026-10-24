/*
 * Copyright 2022 Alibaba Group Holding Ltd.
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

package com.aliyun.fastmodel.transform.mc.parser;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.AutoPartitionedBy;
import com.aliyun.fastmodel.transform.mc.MaxComputeTransformer;
import com.aliyun.fastmodel.transform.mc.context.MaxComputeContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test for parsing MaxCompute AUTO PARTITIONED BY SQL
 *
 * @author Qwen
 */
public class AutoPartitionedTableTest {

    private final MaxComputeTransformer maxComputeTransformer = new MaxComputeTransformer();

    // The SQL statement to test
    String sql = "CREATE TABLE IF NOT EXISTS ingestion_sale_detail_hourly(\n" +
        " shop_name     STRING,\n" +
        " total_price   DOUBLE,\n" +
        " _partitiontime TIMESTAMP)\n" +
        "AUTO PARTITIONED BY (TRUNC_TIME(_partitiontime, 'hour') AS sale_time)\n" +
        "TBLPROPERTIES('ingestion_time_partition'='true');";

    @Test
    public void testAutoPartitionedTable() {

        // Parse the SQL using the MaxCompute transformer
        BaseStatement baseStatement = maxComputeTransformer.reverse(new DialectNode(sql), null);

        // Verify that it was parsed as a CreateTable statement
        assertNotNull("Statement should not be null", baseStatement);
        assertTrue("Statement should be a CreateTable", baseStatement instanceof CreateTable);

        CreateTable createTable = (CreateTable)baseStatement;

        // Verify the table name
        assertEquals("Table name should be 'ingestion_sale_detail_hourly'", "ingestion_sale_detail_hourly", createTable.getQualifiedName().toString());

        // Verify IF NOT EXISTS
        assertTrue("Should have IF NOT EXISTS", createTable.getCreateElement().isNotExists());

        // Verify that the table has the correct columns
        assertEquals("Table should have 3 columns", 3, createTable.getColumnDefines().size());

        // We cannot directly test for AUTO PARTITIONED BY as the internal representation may differ,
        // but the important part is that the parsing completes without errors
        assertNotNull("CREATE TABLE statement parsed successfully", createTable);

        // add the auto partition
        PartitionedBy partitionedBy = createTable.getPartitionedBy();
        AutoPartitionedBy autoPartitionedBy = (AutoPartitionedBy)partitionedBy;
        assertEquals("sale_time", autoPartitionedBy.getAlias().getValue());
        assertEquals("TRUNC_TIME(`_partitiontime`, 'hour')", autoPartitionedBy.getExpression().toString());
    }

    @Test
    public void testTransformAutoPartitionedTable() {
        // Parse the SQL using the MaxCompute transformer
        BaseStatement baseStatement = maxComputeTransformer.reverse(new DialectNode(sql), null);

        DialectNode dialectNode = maxComputeTransformer.transform(baseStatement, MaxComputeContext.builder().build());

        assertEquals("CREATE TABLE IF NOT EXISTS ingestion_sale_detail_hourly\n"
            + "(\n"
            + "   shop_name        STRING,\n"
            + "   total_price      DOUBLE,\n"
            + "   `_partitiontime` TIMESTAMP\n"
            + ")\n"
            + "AUTO PARTITIONED BY\n"
            + "(\n"
            + "   TRUNC_TIME(`_partitiontime`, 'hour') AS sale_time\n"
            + ")\n"
            + "TBLPROPERTIES ('ingestion_time_partition'='true')", dialectNode.getNode());
    }
}